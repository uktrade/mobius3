import array
import asyncio
import ctypes
import enum
import fcntl
import termios
import logging
import os
from pathlib import (
    PurePosixPath,
)
import struct

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

    upload_queue = asyncio.Queue()
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
            for _ in range(0, concurrent_uploads)
        ]

        fd = libc.inotify_init()
        loop.add_reader(fd, handle)
        ensure_watcher(local_root)

    def ensure_watcher(path):
        if path in paths_set:
            return

        wd = libc.inotify_add_watch(fd, path.encode('utf-8'),
                                    InotifyFlags.IN_CLOSE_WRITE |
                                    InotifyFlags.IN_CREATE,
                                    )
        wds_to_path[wd] = path
        paths_set.add(path)

        # By the time we've added a watcher, files or subdirectories may have
        # been created
        for root, dirs, files in os.walk(path):
            for file in files:
                upload_queue.put_nowait(os.path.join(root, file))

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
        upload_queue.put_nowait(path)

    def handle_IN_CREATE(mask, path):
        if mask & InotifyFlags.IN_ISDIR:
            ensure_watcher(path)

    async def file_body(pathname):
        with open(pathname, 'rb') as file:
            for chunk in iter(lambda: file.read(16384), b''):
                yield chunk

    async def upload():
        while True:
            pathname = await upload_queue.get()

            try:
                remote_url = remote_root + '/' + \
                    str(PurePosixPath(pathname).relative_to(local_root))
                content_length = str(os.stat(pathname).st_size).encode()

                code, _, body = await signed_request(
                    b'PUT', remote_url, body=file_body, body_args=(pathname,),
                    headers=((b'content-length', content_length),)
                )
                body_bytes = await buffered(body)
                if code != b'200':
                    raise Exception(code, body_bytes)

            except Exception:
                logger.exception('Exception during upload of %s', pathname)
            finally:
                upload_queue.task_done()

    parent_locals = locals()

    return start, stop
