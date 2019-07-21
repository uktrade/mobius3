import array
import asyncio
import ctypes
import enum
import errno
import fcntl
import termios
import logging
import os
from pathlib import (
    PurePosixPath,
)
import struct
from weakref import (
    WeakValueDictionary,
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


class CancelledUpload(Exception):
    pass


class WeakReferenceableDict(dict):
    pass


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
    IN_MASK_ADD = 0x20000000
    IN_ISDIR = 0x40000000
    IN_ONESHOT = 0x80000000


STRUCT_HEADER = struct.Struct('iIII')


async def get_credentials_from_environment():
    return os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'], ()


def Syncer(local_root, remote_root, remote_region,
           concurrent_uploads=5,
           get_credentials=get_credentials_from_environment,
           get_pool=Pool,
           ):

    loop = asyncio.get_running_loop()
    logger = logging.getLogger('mobius3')

    fd = None
    wds_to_path = {}
    paths_set = set()

    job_queue = asyncio.Queue()

    # A path -> version dict is maintained during queues and uploads. When a
    # path is scheduled to be uploaded, its version is incremented. Before,
    # during, and most importantly after the last read of data for an upload,
    # but _before_ its uploaded, the versions of the path, and its parents are
    # checked to see if they are the latest. If not there was as have been a
    # change to the filesystem, and another upload will be scheduled, so we
    # abort
    path_versions = WeakValueDictionary()
    upload_tasks = []

    request, close_pool = get_pool()
    signed_request = signed(
        request, credentials=get_credentials, service='s3', region=remote_region
    )

    async def start():
        nonlocal upload_tasks
        nonlocal fd
        upload_tasks = [
            asyncio.create_task(upload())
            for i in range(0, concurrent_uploads)
        ]

        fd = libc.inotify_init()
        loop.add_reader(fd, handle)
        ensure_watcher(local_root)

    def ensure_watcher(path):
        if path in paths_set:
            return

        try:
            wd = libc.inotify_add_watch(fd, path.encode('utf-8'),
                                        InotifyFlags.IN_ONLYDIR |
                                        InotifyFlags.IN_CLOSE_WRITE |
                                        InotifyFlags.IN_CREATE,
                                        )
        except OSError:
            if OSError.errno == errno.ENOTDIR:
                return
            raise

        wds_to_path[wd] = path
        paths_set.add(path)

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
        for task in upload_tasks:
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

            wd, mask, _, length = STRUCT_HEADER.unpack_from(raw_bytes, offset)
            offset += STRUCT_HEADER.size
            path = raw_bytes[offset:offset+length].rstrip(b'\0').decode('utf-8')
            offset += length
            raw_bytes = raw_bytes[offset:]
            offset = 0

            flags = [flag for flag in InotifyFlags.__members__.values() if flag & mask]
            for flag in flags:
                try:
                    handler = parent_locals[f'handle_{flag.name}']
                except KeyError:
                    return

                try:
                    handler(mask, wds_to_path[wd] + '/' + path)
                except Exception:
                    logger.exception('Exception during handler %s', path)

    def handle_IN_CLOSE_WRITE(_, path):
        schedule_upload(path)

    def handle_IN_CREATE(mask, path):
        if mask & InotifyFlags.IN_ISDIR:
            ensure_watcher(path)

    def schedule_upload(path):
        path_posix = PurePosixPath(path)
        versions = {
            parent: path_versions.setdefault(parent, default=WeakReferenceableDict(version=0))
            for parent in [path_posix] + list(path_posix.parents)
        }

        versions[path_posix]['version'] += 1

        job_queue.put_nowait({
            'path': path,
            'versions_original': {key: version.copy() for key, version in versions.items()},
            'versions_current': versions,
        })

    async def upload():

        async def file_body():
            uploaded = 0
            with open(pathname, 'rb') as file:
                for chunk in iter(lambda: file.read(16384), b''):
                    # Before the final chunk, but _after_ we read from the
                    # filesystem, we yield to make sure any events have been
                    # processed that mean the file may have been changed
                    uploaded += len(chunk)
                    if uploaded == size:
                        # Hacky: may need a better way to determine that we
                        # have definitely received the latest notifications
                        # for this file (or parent directories)
                        await asyncio.sleep(0.5)

                    if job['versions_current'] != job['versions_original']:
                        raise CancelledUpload()

                    yield chunk

        while True:
            job = await job_queue.get()

            try:
                if job['versions_current'] != job['versions_original']:
                    continue
                pathname = job['path']

                remote_url = remote_root + '/' + \
                    str(PurePosixPath(pathname).relative_to(local_root))
                size = os.stat(pathname).st_size
                content_length = str(size).encode()

                code, _, body = await signed_request(
                    b'PUT', remote_url, body=file_body,
                    headers=((b'content-length', content_length),)
                )
                body_bytes = await buffered(body)
                if code != b'200':
                    raise Exception(code, body_bytes)

            except Exception as exception:
                if not isinstance(exception.__cause__, CancelledUpload):
                    logger.exception('Exception during upload of %s', pathname)

            finally:
                job_queue.task_done()

    parent_locals = locals()

    return start, stop
