import array
import asyncio
import ctypes
import enum
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
    empty_async_iterator,
)
from lowhaio_aws_sigv4_unsigned_payload import (
    signed,
)


libc = ctypes.CDLL('libc.so.6', use_errno=True)
libc.inotify_init.argtypes = []
libc.inotify_add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]


def call_libc(func, *args):
    value = func(*args)
    latest_errno = ctypes.set_errno(0)
    if latest_errno:
        raise OSError(latest_errno, os.strerror(latest_errno))
    return value


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
    InotifyFlags.IN_CREATE | \
    InotifyFlags.IN_DELETE | \
    InotifyFlags.IN_MOVED_TO | \
    InotifyFlags.IN_MOVED_FROM


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

    # A cache of the layout of objects used for renames
    layout_cache = {
        'type': 'directory',
        'children': {},
    }

    request, close_pool = get_pool()
    signed_request = signed(
        request, credentials=get_credentials, service='s3', region=remote_region
    )

    def add_file_to_layout_cache(path):
        path_posix = PurePosixPath(path)
        directory = layout_cache
        for parent in reversed(list(path_posix.parents)):
            directory = directory['children'].setdefault(parent.name, {
                'type': 'directory',
                'children': {},
            })
        directory['children'][path_posix.name] = {
            'type': 'file',
        }

    def remove_file_from_layout_cache(path):
        path_posix = PurePosixPath(path)
        directory = layout_cache
        for parent in reversed(list(path_posix.parents)):
            directory = directory['children'][parent.name]

        del directory['children'][path_posix.name]

    def layout_cache_directory(path):
        path_posix = PurePosixPath(path)
        directory = layout_cache
        for parent in reversed(list(path_posix.parents)):
            directory = directory['children'][parent.name]
        return directory['children'][path_posix.name]

    async def start():
        nonlocal tasks
        nonlocal fd
        tasks = [
            asyncio.create_task(process_jobs())
            for i in range(0, concurrent_uploads)
        ]

        fd = call_libc(libc.inotify_init)
        loop.add_reader(fd, handle)
        ensure_watcher(local_root)

    def ensure_watcher(path):
        try:
            wd = call_libc(libc.inotify_add_watch, fd, path.encode('utf-8'), WATCHED_EVENTS)
        except (NotADirectoryError, FileNotFoundError):
            return

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
            item_type = 'dir' if mask & InotifyFlags.IN_ISDIR else 'file'
            for flag in flags:
                try:
                    handler = parent_locals[f'handle__{item_type}__{flag.name}']
                except KeyError:
                    continue

                try:
                    handler(wd, full_path)
                except Exception:
                    logger.exception('Exception during handler %s', path)

    def handle__file__IN_CLOSE_WRITE(_, path):
        schedule_upload(path)

    def handle__dir__IN_CREATE(_, path):
        ensure_watcher(path)

    def handle__file__IN_DELETE(_, path):
        # Correctness does not depend on this bump: it's an optimisation
        # that ensures we abandon any upload of this path ahead of us
        # in the queue
        bump_content_version(path)
        schedule_delete(path)

    def handle__dir__IN_IGNORED(wd, _):
        del wds_to_path[wd]

    def handle__file__IN_MODIFY(_, path):
        bump_content_version(path)

    def handle__dir__IN_MOVED_FROM(_, path):
        # Directory nesting not likely to be large
        def recursive_delete(prefix, directory):
            for child_name, child in list(directory['children'].items()):
                if child['type'] == 'file':
                    schedule_delete(prefix + '/' + child_name)
                else:
                    recursive_delete(prefix + '/' + child_name, child)

        try:
            cache_directory = layout_cache_directory(path)
        except KeyError:
            # We may be moving from something not yet watched
            pass
        else:
            recursive_delete(path, cache_directory)

    def handle__file__IN_MOVED_FROM(_, path):
        schedule_delete(path)

    def handle__dir__IN_MOVED_TO(_, path):
        ensure_watcher(path)

    def handle__file__IN_MOVED_TO(_, path):
        schedule_upload(path)

    def get_content_version(path):
        return content_versions.setdefault(path, default=WeakReferenceableDict(version=0))

    def bump_content_version(path):
        get_content_version(path)['version'] += 1

    def get_lock(path):
        return path_locks.setdefault(path, default=FifoLock())

    def schedule_upload(path):
        version_current = get_content_version(path)
        version_original = version_current.copy()

        async def function():
            await upload(path, version_current, version_original)

        add_file_to_layout_cache(path)
        job_queue.put_nowait(function)

    def schedule_delete(path):
        async def function():
            await delete(path)

        remove_file_from_layout_cache(path)
        job_queue.put_nowait(function)

    async def process_jobs():
        while True:
            try:
                job = await job_queue.get()
                try:
                    await job()
                finally:
                    job_queue.task_done()

            except Exception as exception:
                if isinstance(exception, asyncio.CancelledError):
                    raise
                if (
                        not isinstance(exception, FileNotFoundError) and
                        not isinstance(exception.__cause__, FileContentChanged)
                ):
                    logger.exception('Exception during %s', job)

    async def upload(path, content_version_current, content_version_original):
        async def flush_events():
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

        async def file_body():
            with open(path, 'rb') as file:

                for is_last, chunk in with_is_last(iter(lambda: file.read(16384), b'')):
                    if is_last:
                        await flush_events()

                    if content_version_current != content_version_original:
                        raise FileContentChanged()

                    yield chunk

        content_length = str(os.stat(path).st_size).encode()
        await locked_request(b'PUT', path, body=file_body,
                             headers=((b'content-length', content_length),))

    async def delete(path):
        await locked_request(b'DELETE', path)

    async def locked_request(method, path, headers=(), body=empty_async_iterator):
        remote_url = remote_root + '/' + str(PurePosixPath(path).relative_to(local_root))

        async with get_lock(path)(Mutex):
            code, _, body = await signed_request(method, remote_url, headers=headers, body=body)
            body_bytes = await buffered(body)

        if code != b'200':
            raise Exception(code, body_bytes)

    parent_locals = locals()

    return start, stop
