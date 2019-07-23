import array
import asyncio
import ctypes
import enum
import errno
import fcntl
import termios
import logging
import os
import uuid
from pathlib import (
    PurePosixPath,
)
import struct
from weakref import (
    WeakValueDictionary,
)

from fifolock import (
    FifoLock,
)
from lowhaio import (
    Pool,
    buffered,
)
from lowhaio_aws_sigv4_unsigned_payload import (
    signed,
)


libc = ctypes.cdll.LoadLibrary('libc.so.6')
libc.inotify_init.argtypes = []
libc.inotify_add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]


class FileContentChanged(Exception):
    pass


class WeakReferenceableDict(dict):
    pass


class Mutex(asyncio.Future):
    @staticmethod
    def is_compatible(holds):
        return not holds[Mutex]


class InotifyFlags(enum.IntEnum):
    # Can watch for these events
    IN_ACCESS = 0x00000001
    IN_MODIFY = 0x00000002
    IN_ATTRIB = 0x00000004
    IN_CLOSE_WRITE = 0x00000008
    IN_CLOSE_NOWRITE = 0x00000010
    IN_OPEN = 0x00000020
    IN_MOVED_FROM = 0x00000040
    IN_MOVED_TO = 0x00000080
    IN_CREATE = 0x00000100
    IN_DELETE = 0x00000200
    IN_DELETE_SELF = 0x00000400
    IN_MOVE_SELF = 0x00000800

    # Events sent by the kernel without explicitly watching for them
    IN_UNMOUNT = 0x00002000
    IN_Q_OVERFLOW = 0x00004000
    IN_IGNORED = 0x00008000

    # Flags
    IN_ONLYDIR = 0x01000000
    IN_DONT_FOLLOW = 0x02000000
    IN_EXCL_UNLINK = 0x04000000
    IN_MASK_CREATE = 0x10000000   # Only from Linux 4.18, Docker Desktop uses 4.9
    IN_MASK_ADD = 0x20000000
    IN_ISDIR = 0x40000000
    IN_ONESHOT = 0x80000000


WATCHED_EVENTS = \
    InotifyFlags.IN_ONLYDIR | \
    InotifyFlags.IN_MODIFY | \
    InotifyFlags.IN_CLOSE_WRITE | \
    InotifyFlags.IN_CREATE


EVENT_HEADER = struct.Struct('iIII')


async def get_credentials_from_environment():
    return os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'], ()


def Syncer(
        local_root, remote_root, remote_region,
        concurrent_uploads=5,
        get_credentials=get_credentials_from_environment,
        get_pool=Pool,
        flush_file_root='.__mobius3__',
):

    loop = asyncio.get_running_loop()
    logger = logging.getLogger('mobius3')

    # The file descriptor returned from inotify_init
    fd = None

    # Watch descriptors to paths. A notification returns only a relative
    # path to its watch descriptor path: these are used to find the full
    # path of any notified-on files
    wds_to_path = {}

    # Uploads are initiated in the order received
    job_queue = asyncio.Queue()

    # A path -> version dict is maintained during queues and uploads. When a
    # path is scheduled to be uploaded, its version is incremented. Before,
    # during, and most importantly after the last read of data for an upload,
    # but _before_ its uploaded, the versions of the path, and its parents are
    # checked to see if they are the latest. If not there was as have been a
    # change to the filesystem, and another upload will be scheduled, so we
    # abort
    content_versions = WeakValueDictionary()

    # The asyncio task pool that performs the uploads
    tasks = []

    # Before completing an upload, we force a flush of the event queue for
    # the uploads directory to ensure that we have processed any change events
    flushes = WeakValueDictionary()

    # To prevent concurrent HTTP requests on the same files where order of
    # receipt by S3 cannot be guarenteed, we wrap each request by a lock
    path_locks = WeakValueDictionary()

    request, close_pool = get_pool()
    signed_request = signed(
        request, credentials=get_credentials, service='s3', region=remote_region
    )

    async def start():
        nonlocal tasks
        nonlocal fd
        tasks = [
            asyncio.create_task(process_jobs())
            for i in range(0, concurrent_uploads)
        ]

        fd = libc.inotify_init()
        loop.add_reader(fd, handle)
        ensure_watcher(local_root)

    def ensure_watcher(path):
        try:
            wd = libc.inotify_add_watch(fd, path.encode('utf-8'), WATCHED_EVENTS)
        except OSError:
            if OSError.errno == errno.ENOTDIR:
                return
            raise

        wds_to_path[wd] = path

        # By the time we've added a watcher, files or subdirectories may have
        # been created
        for root, dirs, files in os.walk(path):
            for file in files:
                schedule_upload(os.path.join(root, file))

            for directory in dirs:
                ensure_watcher(os.path.join(root, directory))

    async def stop():
        loop.remove_reader(fd)
        os.close(fd)
        await close_pool()
        for task in tasks:
            task.cancel()
        await asyncio.sleep(0)

    def handle():
        FIONREAD_output = array.array('i', [0])
        fcntl.ioctl(fd, termios.FIONREAD, FIONREAD_output)
        bytes_to_read = FIONREAD_output[0]
        raw_bytes = os.read(fd, bytes_to_read)

        offset = 0
        while True:
            if not raw_bytes:
                break

            wd, mask, _, length = EVENT_HEADER.unpack_from(raw_bytes, offset)
            offset += EVENT_HEADER.size
            path = raw_bytes[offset:offset+length].rstrip(b'\0').decode('utf-8')
            offset += length
            raw_bytes = raw_bytes[offset:]
            offset = 0

            full_path = wds_to_path[wd] + '/' + path

            if path.startswith(flush_file_root):
                try:
                    flush = flushes[full_path]
                except KeyError:
                    continue
                else:
                    flush.set()
                    continue

            flags = [flag for flag in InotifyFlags.__members__.values() if flag & mask]
            for flag in flags:
                try:
                    handler = parent_locals[f'handle_{flag.name}']
                except KeyError:
                    break

                try:
                    handler(full_path)
                except Exception:
                    logger.exception('Exception during handler %s', path)

    def handle_IN_CLOSE_WRITE(path):
        schedule_upload(path)

    def handle_IN_CREATE(path):
        ensure_watcher(path)

    def handle_IN_MODIFY(path):
        bump_content_version(path)

    def get_content_version(path):
        return content_versions.setdefault(path, default=WeakReferenceableDict(version=0))

    def bump_content_version(path):
        get_content_version(path)['version'] += 1

    def get_lock(path):
        return path_locks.setdefault(path, default=FifoLock())

    def schedule_upload(path):
        version_current = get_content_version(path)
        version_original = version_current.copy()

        job_queue.put_nowait({
            'function': upload,
            'kwargs': {
                'path': path,
                'content_version_original': version_original,
                'content_version_current': version_current,
            }
        })

    async def flush_events(path):
        flush_path = PurePosixPath(path).parent / (flush_file_root + uuid.uuid4().hex)
        event = asyncio.Event()
        flushes[str(flush_path)] = event
        with open(str(flush_path), 'w'):
            pass
        await event.wait()

    def with_is_last(iterable):
        try:
            last = next(iterable)
        except StopIteration:
            return

        for val in iterable:
            yield False, last
            last = val

        yield True, last

    async def process_jobs():
        while True:
            try:
                job = await job_queue.get()
                try:
                    await job['function'](**job['kwargs'])
                finally:
                    job_queue.task_done()

            except Exception as exception:
                if isinstance(exception, asyncio.CancelledError):
                    raise
                if not isinstance(exception.__cause__, FileContentChanged):
                    logger.exception('Exception during %s', job)

    async def upload(path, content_version_current, content_version_original):
        async def file_body():
            with open(path, 'rb') as file:

                for is_last, chunk in with_is_last(iter(lambda: file.read(16384), b'')):
                    if is_last:
                        await flush_events(path)

                    if content_version_current != content_version_original:
                        raise FileContentChanged()

                    yield chunk

        remote_url = remote_root + '/' + \
            str(PurePosixPath(path).relative_to(local_root))
        content_length = str(os.stat(path).st_size).encode()

        async with get_lock(path)(Mutex):
            code, _, body = await signed_request(
                b'PUT', remote_url, body=file_body,
                headers=((b'content-length', content_length),)
            )
            body_bytes = await buffered(body)

        if code != b'200':
            raise Exception(code, body_bytes)

    parent_locals = locals()

    return start, stop
