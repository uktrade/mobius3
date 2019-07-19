import asyncio
import logging
import os
from pathlib import (
    PurePosixPath,
)

from lowhaio import (
    Pool,
    buffered,
)
from lowhaio_aws_sigv4_unsigned_payload import (
    signed,
)
import pyinotify  # pylint: disable=import-error


async def get_credentials_from_environment():
    return os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'], ()


def Syncer(local_root, remote_root, remote_region,
           concurrent_uploads=5,
           get_credentials=get_credentials_from_environment,
           get_pool=Pool,
           ):

    loop = asyncio.get_running_loop()
    logger = logging.getLogger('mobius3')
    upload_queue = asyncio.Queue()
    upload_tasks = None

    request, _ = get_pool()
    signed_request = signed(
        request, credentials=get_credentials, service='s3', region=remote_region
    )

    async def start():
        nonlocal upload_tasks
        upload_tasks = [
            asyncio.create_task(upload())
            for _ in range(0, concurrent_uploads)
        ]

        wm = pyinotify.WatchManager()
        pyinotify.AsyncioNotifier(wm, loop, default_proc_fun=handle)
        wm.add_watch(local_root, pyinotify.ALL_EVENTS, rec=True)

    async def stop():
        pass

    def handle(event):
        try:
            handler = parent_locals[f'handle_{event.maskname}']
        except KeyError:
            return

        try:
            handler(event)
        except Exception:
            logger.exception('Exception during handler %s', event)

    def handle_IN_CLOSE_WRITE(event):
        upload_queue.put_nowait(event.pathname)

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
