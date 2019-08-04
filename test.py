import asyncio
from datetime import (
    datetime,
)
import os
import re
import shutil
import ssl
import sys
import unittest
import uuid

from aiodnsresolver import (
    Resolver,
)
from aiohttp import (
    web,
)
from lowhaio import (
    Pool,
    buffered,
    streamed,
)
from lowhaio_aws_sigv4_unsigned_payload import (
    signed,
)
from mobius3 import (
    Syncer,
)


def async_test(func):
    def wrapper(*args, **kwargs):
        future = func(*args, **kwargs)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(future)
    return wrapper


class TestIntegration(unittest.TestCase):

    def add_async_cleanup(self, coroutine, *args):
        loop = asyncio.get_event_loop()
        self.addCleanup(loop.run_until_complete, coroutine(*args))

    @async_test
    async def test_download_file_at_start_then_upload(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        filename_1 = str(uuid.uuid4())
        code, headers, body = await put_body(request, f'prefix/{filename_1}', b'some-bytes')
        self.assertEqual(code, b'200')
        await buffered(body)

        date = dict((key.lower(), value) for key, value in headers)[b'date']
        date_ts = datetime.strptime(date.decode(), '%a, %d %b %Y %H:%M:%S %Z').timestamp()

        # Make sure time progresses at least one second, to test that there
        # is some code setting mtime
        await asyncio.sleep(1)

        start, stop = syncer_for('/s3-home-folder', prefix='prefix/')

        await start()

        with open(f'/s3-home-folder/{filename_1}', 'rb') as file:
            body_bytes = file.read()

        self.assertEqual(body_bytes, b'some-bytes')
        self.assertEqual(date_ts, os.path.getmtime(f'/s3-home-folder/{filename_1}'))

        filename_2 = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename_2}', 'wb') as file:
            file.write(b'more-bytes')

        await stop()

        self.assertEqual(await object_body(request, f'prefix/{filename_2}'), b'more-bytes')

    @async_test
    async def test_download_nested_files_at_start(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        filename_1 = str(uuid.uuid4())
        filename_2 = str(uuid.uuid4())
        directory = str(uuid.uuid4())
        code, _, body = await put_body(request, f'prefix/{directory}/{filename_1}', b'some-bytes')
        self.assertEqual(code, b'200')
        await buffered(body)
        code, _, body = await put_body(request, f'prefix/{directory}/{filename_2}', b'more-bytes')
        self.assertEqual(code, b'200')
        await buffered(body)

        start, stop = syncer_for('/s3-home-folder', prefix='prefix/')
        self.add_async_cleanup(stop)

        await start()

        with open(f'/s3-home-folder/{directory}/{filename_1}', 'rb') as file:
            body_bytes = file.read()
        self.assertEqual(body_bytes, b'some-bytes')
        with open(f'/s3-home-folder/{directory}/{filename_2}', 'rb') as file:
            body_bytes = file.read()
        self.assertEqual(body_bytes, b'more-bytes')

    @async_test
    async def test_exclude_local_after_start(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder', exclude_local=r'.*to-exclude.*')
        self.add_async_cleanup(stop)
        await start()

        filename_1 = str(uuid.uuid4())
        filename_2 = str(uuid.uuid4()) + 'to-exclude'
        dirname_1 = str(uuid.uuid4()) + 'to-exclude'
        filename_3 = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename_1}', 'wb') as file:
            file.write(b'some-bytes-a')
        with open(f'/s3-home-folder/{filename_2}', 'wb') as file:
            file.write(b'some-bytes-b')
        os.mkdir(f'/s3-home-folder/{dirname_1}')
        with open(f'/s3-home-folder/{dirname_1}/{filename_3}', 'wb') as file:
            file.write(b'some-bytes-b')

        await asyncio.sleep(1)

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(await object_body(request, filename_1), b'some-bytes-a')
        self.assertEqual(await object_code(request, filename_2), b'404')
        self.assertEqual(await object_code(request, f'{dirname_1}/{filename_3}'), b'404')

        # Minio seems to return a 200 for all folders, but this _should_ assertEqual for S3 proper
        # self.assertEqual(await object_code(request, f'{dirname_1}/'), b'404')

    @async_test
    async def test_exclude_remote_at_start(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        filename_1 = str(uuid.uuid4())
        filename_2 = str(uuid.uuid4())
        directory = str(uuid.uuid4())
        code, _, body = await put_body(request, f'prefix/{filename_1}/.checkpoints/check_1',
                                       b'some-inner-bytes')
        self.assertEqual(code, b'200')
        await buffered(body)
        code, _, body = await put_body(request, f'prefix/{directory}/{filename_2}', b'more-bytes')
        self.assertEqual(code, b'200')
        await buffered(body)

        start, stop = syncer_for('/s3-home-folder', prefix='prefix/',
                                 exclude_remote=re.compile(r'.*\.checkpoints/.*'))
        self.add_async_cleanup(stop)

        await start()

        self.assertFalse(os.path.exists(f'/s3-home-folder/{filename_1}/.checkpoints/check_1'))

        with open(f'/s3-home-folder/{directory}/{filename_2}', 'rb') as file:
            body_bytes = file.read()
        self.assertEqual(body_bytes, b'more-bytes')

    @async_test
    async def test_download_file_after_start(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        start, stop = syncer_for(
            '/s3-home-folder', prefix='prefix/',
            download_interval=1)
        self.add_async_cleanup(stop)

        await start()

        filename_1 = str(uuid.uuid4())
        code, headers, body = await put_body(request, f'prefix/{filename_1}', b'some-bytes')
        self.assertEqual(code, b'200')

        date = dict((key.lower(), value) for key, value in headers)[b'date']
        date_ts = datetime.strptime(date.decode(), '%a, %d %b %Y %H:%M:%S %Z').timestamp()

        await buffered(body)

        await asyncio.sleep(2)

        with open(f'/s3-home-folder/{filename_1}', 'rb') as file:
            body_bytes = file.read()

        self.assertEqual(body_bytes, b'some-bytes')
        self.assertEqual(date_ts, os.path.getmtime(f'/s3-home-folder/{filename_1}'))

        await asyncio.sleep(2)

        # Ensure the file isn't re-downloaded, or at least if it is, it has
        # the correct mtime
        self.assertEqual(date_ts, os.path.getmtime(f'/s3-home-folder/{filename_1}'))

    @async_test
    async def test_download_nested_file_after_start(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        start, stop = syncer_for(
            '/s3-home-folder', prefix='prefix/',
            download_interval=1)
        self.add_async_cleanup(stop)

        await start()

        dirname_1 = str(uuid.uuid4())
        filename_1 = str(uuid.uuid4())
        code, headers, body = await put_body(
            request, f'prefix/{dirname_1}/{filename_1}',
            b'some-bytes')
        self.assertEqual(code, b'200')
        await buffered(body)
        date = dict((key.lower(), value) for key, value in headers)[b'date']

        await asyncio.sleep(2)

        # Ensure the modified date stays the same to check that a second upload
        # has not occured
        code, headers, body = await object_triple(request, f'prefix/{dirname_1}/{filename_1}')
        self.assertEqual(code, b'200')
        await buffered(body)
        modified = dict((key.lower(), value) for key, value in headers)[b'last-modified']
        self.assertEqual(modified, date)

        with open(f'/s3-home-folder/{dirname_1}/{filename_1}', 'rb') as file:
            body_bytes = file.read()

        self.assertEqual(body_bytes, b'some-bytes')

        dirname_2 = str(uuid.uuid4())
        os.rename(f'/s3-home-folder/{dirname_1}', f'/s3-home-folder/{dirname_2}')

        await asyncio.sleep(2)

        self.assertEqual(await object_code(request, f'prefix/{dirname_1}/{filename_1}'), b'404')
        self.assertEqual(await object_body(
            request, f'prefix/{dirname_2}/{filename_1}'), b'some-bytes')

    @async_test
    async def test_download_directory_after_start(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)

        dirname_1 = str(uuid.uuid4())
        dirname_2 = str(uuid.uuid4())
        dirname_3 = str(uuid.uuid4())

        start, stop = Syncer(
            '/s3-home-folder', 'http://localhost:8080/my-bucket/', 'us-east-1',
        )
        self.add_async_cleanup(stop)

        # minio does not support keys with trailing slashes, so we fire up our
        # own mock S3
        async def handle_list(_):
            body = f'''<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Contents>
                    <Key>{dirname_1}/</Key>
                    <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                </Contents>
                <Contents>
                    <Key>{dirname_2}/{dirname_3}/</Key>
                    <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                </Contents>
            </ListBucketResult>'''.encode()
            return web.Response(status=200, body=body)

        async def handle_dir(_):
            return web.Response(status=200, headers={
                'last-modified': 'Fri, 10 May 2019 06:53:17 GMT',
                'etag': '"fba9dede5f27731c9771645a39863328"',
            }, body=b'')

        app = web.Application()
        app.add_routes([
            web.get(f'/my-bucket/', handle_list),
            web.get(f'/my-bucket/{dirname_1}/', handle_dir),
            web.get(f'/my-bucket/{dirname_2}/', handle_dir),
            web.get(f'/my-bucket/{dirname_2}/{dirname_3}/', handle_dir),
            # It's not great that downloads then attempt to re-upload
            web.put(f'/my-bucket/', handle_list),
            web.put(f'/my-bucket/{dirname_1}/', handle_dir),
            web.put(f'/my-bucket/{dirname_2}/', handle_dir),
            web.put(f'/my-bucket/{dirname_2}/{dirname_3}/', handle_dir),
        ])
        runner = web.AppRunner(app)
        await runner.setup()
        self.add_async_cleanup(runner.cleanup)
        site = web.TCPSite(runner, '0.0.0.0', 8080)
        await site.start()

        await start()

        self.assertTrue(os.path.isdir(f'/s3-home-folder/{dirname_1}'))
        self.assertTrue(os.path.isdir(f'/s3-home-folder/{dirname_2}/{dirname_3}'))
        self.assertEqual(os.path.getmtime(f'/s3-home-folder/{dirname_2}/{dirname_3}'),
                         1557471197.0)

    @async_test
    async def test_delete_downloaded_directory(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)

        dirname_1 = str(uuid.uuid4())
        dirname_2 = str(uuid.uuid4())

        with_dirs = f'''<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Contents>
                    <Key>{dirname_1}/{dirname_2}/</Key>
                    <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                </Contents>
            </ListBucketResult>'''.encode()

        without_dirs = f'''<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            </ListBucketResult>'''.encode()

        list_body = with_dirs
        get_code = 200
        get_headers = {
            'last-modified': 'Fri, 10 May 2019 06:53:17 GMT',
            'etag': '"fba9dede5f27731c9771645a39863328"',
        }

        start, stop = Syncer(
            '/s3-home-folder', 'http://localhost:8080/my-bucket/', 'us-east-1',
            local_modification_persistance=1,
            download_interval=1,
        )
        self.add_async_cleanup(stop)

        # minio does not support keys with trailing slashes, so we fire up our
        # own mock S3
        async def handle_list(_):
            return web.Response(status=200, body=list_body)

        async def handle_dir(_):
            return web.Response(status=get_code, headers=get_headers, body=b'')

        app = web.Application()
        app.add_routes([
            web.get(f'/my-bucket/', handle_list),
            web.get(f'/my-bucket/{dirname_1}/', handle_dir),
            web.get(f'/my-bucket/{dirname_1}/{dirname_2}/', handle_dir),
            # It's not great that downloads then attempt to re-upload
            web.put(f'/my-bucket/{dirname_1}/', handle_dir),
            web.put(f'/my-bucket/{dirname_1}/{dirname_2}/', handle_dir),
        ])
        runner = web.AppRunner(app)
        await runner.setup()
        self.add_async_cleanup(runner.cleanup)
        site = web.TCPSite(runner, '0.0.0.0', 8080)
        await site.start()

        await start()

        self.assertTrue(os.path.isdir(f'/s3-home-folder/{dirname_1}/{dirname_2}'))

        await asyncio.sleep(1)

        # Simulate files having been deleted on S3
        list_body = without_dirs
        get_code = 404
        get_headers = {}

        await asyncio.sleep(2)

        self.assertFalse(os.path.exists(f'/s3-home-folder/{dirname_1}'))

    @async_test
    async def test_download_file_not_done_during_local_persistance(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        start, stop = syncer_for(
            '/s3-home-folder', prefix='prefix/',
            local_modification_persistance=4,
            download_interval=1,
        )
        self.add_async_cleanup(stop)

        await start()

        filename_1 = str(uuid.uuid4())

        with open(f'/s3-home-folder/{filename_1}', 'wb') as file:
            file.write(b'some-bytes')

        # Wait for the transfer to take place
        await asyncio.sleep(1)

        # The remote file is overridden
        code, _, body = await put_body(request, f'prefix/{filename_1}', b'some-remote-bytes')
        self.assertEqual(code, b'200')
        await buffered(body)

        # Ensure that the local file is not yet overridden
        await asyncio.sleep(2)
        with open(f'/s3-home-folder/{filename_1}', 'rb') as file:
            body_bytes = file.read()
        self.assertEqual(body_bytes, b'some-bytes')

        # Ensure that the local file is overritten
        await asyncio.sleep(6)

        with open(f'/s3-home-folder/{filename_1}', 'rb') as file:
            body_bytes = file.read()
        self.assertEqual(body_bytes, b'some-remote-bytes')

    @async_test
    async def test_download_file_repeated_remote_changes(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        start, stop = syncer_for(
            '/s3-home-folder', prefix='prefix/',
            download_interval=1,
            local_modification_persistance=1
        )
        self.add_async_cleanup(stop)

        await start()

        filename_1 = str(uuid.uuid4())

        code, _, body = await put_body(request, f'prefix/{filename_1}', b'some-remote-bytes-a')
        self.assertEqual(code, b'200')
        await buffered(body)

        # Ensure that the local file isoverridden
        await asyncio.sleep(2)

        with open(f'/s3-home-folder/{filename_1}', 'rb') as file:
            body_bytes = file.read()
        self.assertEqual(body_bytes, b'some-remote-bytes-a')

        code, _, body = await put_body(request, f'prefix/{filename_1}', b'some-remote-bytes-b')
        self.assertEqual(code, b'200')
        await buffered(body)

        # Ensure that the local file is overritten
        await asyncio.sleep(2)

        with open(f'/s3-home-folder/{filename_1}', 'rb') as file:
            body_bytes = file.read()
        self.assertEqual(body_bytes, b'some-remote-bytes-b')

    @async_test
    async def test_single_small_file_uploaded(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop)
        await start()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(await object_body(request, filename), b'some-bytes')

    @async_test
    async def test_directory_uploaded_after_start_then_manipulated(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)

        dirname_1 = str(uuid.uuid4())
        dirname_2 = str(uuid.uuid4())
        dirname_3 = str(uuid.uuid4())
        dirname_4 = str(uuid.uuid4())

        start, stop = Syncer(
            '/s3-home-folder', 'http://localhost:8080/my-bucket/', 'us-east-1',
        )
        self.add_async_cleanup(stop)

        async def handle_list(_):
            body = f'''<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            </ListBucketResult>'''.encode()
            return web.Response(status=200, body=body)

        delete_paths = []
        put_paths = []

        async def handle_dir_delete(request):
            delete_paths.append(request.url.path)

            return web.Response(status=200, body=b'')

        async def handle_dir_put(request):
            put_paths.append(request.url.path)

            return web.Response(status=200, headers={
                'last-modified': 'Fri, 10 May 2019 06:53:17 GMT',
                'etag': '"fba9dede5f27731c9771645a39863328"',
            }, body=b'')

        app = web.Application()
        app.add_routes([
            web.get(f'/my-bucket/', handle_list),
            web.put(f'/my-bucket/{dirname_1}/', handle_dir_put),
            web.put(f'/my-bucket/{dirname_2}/', handle_dir_put),
            web.put(f'/my-bucket/{dirname_2}/{dirname_3}/', handle_dir_put),
            web.put(f'/my-bucket/{dirname_4}/', handle_dir_put),
            web.put(f'/my-bucket/{dirname_4}/{dirname_3}/', handle_dir_put),
            web.delete(f'/my-bucket/{dirname_1}/', handle_dir_delete),
            web.delete(f'/my-bucket/{dirname_2}/', handle_dir_delete),
            web.delete(f'/my-bucket/{dirname_2}/{dirname_3}/', handle_dir_delete),
        ])
        runner = web.AppRunner(app)
        await runner.setup()
        self.add_async_cleanup(runner.cleanup)
        site = web.TCPSite(runner, '0.0.0.0', 8080)
        await site.start()

        await start()

        os.mkdir(f'/s3-home-folder/{dirname_1}')
        os.mkdir(f'/s3-home-folder/{dirname_2}')
        os.mkdir(f'/s3-home-folder/{dirname_2}/{dirname_3}')

        await asyncio.sleep(1)

        self.assertIn(f'/my-bucket/{dirname_1}/', put_paths)
        self.assertIn(f'/my-bucket/{dirname_2}/', put_paths)
        self.assertIn(f'/my-bucket/{dirname_2}/{dirname_3}/', put_paths)

        os.rmdir(f'/s3-home-folder/{dirname_1}')
        os.rename(f'/s3-home-folder/{dirname_2}', f'/s3-home-folder/{dirname_4}')

        await asyncio.sleep(1)

        self.assertIn(f'/my-bucket/{dirname_1}/', delete_paths)
        self.assertIn(f'/my-bucket/{dirname_2}/', delete_paths)
        self.assertIn(f'/my-bucket/{dirname_2}/{dirname_3}/', delete_paths)
        self.assertIn(f'/my-bucket/{dirname_4}/', put_paths)
        self.assertIn(f'/my-bucket/{dirname_4}/{dirname_3}/', put_paths)

    @async_test
    async def test_single_medium_file_uploaded(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop)
        await start()

        filename = str(uuid.uuid4())
        contents = str(uuid.uuid4()).encode() * 100000
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(contents)

        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(await object_body(request, filename), contents)

    @async_test
    async def test_larger_numbers_of_files(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder',
                                 local_modification_persistance=2, download_interval=1)
        self.add_async_cleanup(stop)
        await start()

        filenames_contents = sorted([
            (str(uuid.uuid4()), str(uuid.uuid4()).encode())
            for i in range(0, 2500)
        ])

        loop = asyncio.get_running_loop()
        start = loop.time()
        for filename, contents in filenames_contents:
            with open(f'/s3-home-folder/{filename}', 'wb') as file:
                file.write(contents)

        await asyncio.sleep(4)

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)
        for filename, contents in filenames_contents:
            self.assertEqual(await object_body(request, filename), contents)

        last_filename = filenames_contents[-1][0]
        _, _, body = await put_body(request, last_filename, b'some-bytes')
        await buffered(body)

        await asyncio.sleep(7)

        with open(f'/s3-home-folder/{last_filename}', 'rb') as file:
            body_bytes = file.read()
        self.assertEqual(body_bytes, b'some-bytes')

    @async_test
    async def test_files_deleted_remotely(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder',
                                 local_modification_persistance=1, download_interval=1)
        self.add_async_cleanup(stop)
        await start()

        filenames_contents = sorted([
            (str(uuid.uuid4()), str(uuid.uuid4()).encode())
            for _ in range(0, 5)
        ])

        loop = asyncio.get_running_loop()
        start = loop.time()
        for filename, contents in filenames_contents:
            with open(f'/s3-home-folder/{filename}', 'wb') as file:
                file.write(contents)

        await asyncio.sleep(2)

        last_filename = filenames_contents[-1][0]

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)
        _, _, body = await delete_object(request, f'{last_filename}')
        await buffered(body)

        await asyncio.sleep(2)

        for filename, content in filenames_contents[:-1]:
            with open(f'/s3-home-folder/{filename}', 'rb') as file:
                body_bytes = file.read()
            self.assertEqual(body_bytes, content)

        self.assertFalse(os.path.exists(f'/s3-home-folder/{last_filename}'))

    @async_test
    async def test_file_upload_preserves_mtime(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop_a = syncer_for('/s3-home-folder')
        stopped = False

        async def stop_once():
            nonlocal stopped
            if stopped:
                return
            stopped = True
            await stop_a()
        self.add_async_cleanup(stop_once)
        await start()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        mtime_1 = os.path.getmtime(f'/s3-home-folder/{filename}')

        await stop_once()
        await delete_dir()

        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)

        await asyncio.sleep(1)

        start, stop_b = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop_b)
        await start()

        await asyncio.sleep(1)

        mtime_2 = os.path.getmtime(f'/s3-home-folder/{filename}')

        self.assertEqual(mtime_1, mtime_2)

    @async_test
    async def test_single_small_file_uploaded_emoji(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop)
        await start()

        filename = str(uuid.uuid4()) + '_🍰'
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(await object_body(request, filename), b'some-bytes')

    @async_test
    async def test_single_empty_file_uploaded(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop)
        await start()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb'):
            pass

        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(await object_body(request, filename), b'')

    @async_test
    async def test_file_inside_directory_after_delay(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop)
        await start()

        directory_name = str(uuid.uuid4())
        filename = str(uuid.uuid4())
        os.mkdir('/s3-home-folder/' + directory_name)

        await asyncio.sleep(0.1)

        with open(f'/s3-home-folder/{directory_name}/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(await object_body(request, f'{directory_name}/{filename}'), b'some-bytes')

    @async_test
    async def test_file_inside_directory_immediate(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop)
        await start()

        directory_name = str(uuid.uuid4())
        filename = str(uuid.uuid4())
        os.mkdir('/s3-home-folder/' + directory_name)

        with open(f'/s3-home-folder/{directory_name}/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(await object_body(request, f'{directory_name}/{filename}'), b'some-bytes')

    @async_test
    async def test_file_inside_nested_directory_immediate_after_previous_deleted(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop)
        await start()

        directory_1 = str(uuid.uuid4())
        directory_2 = str(uuid.uuid4())
        filename = str(uuid.uuid4())
        os.mkdir('/s3-home-folder/' + directory_1)
        os.mkdir('/s3-home-folder/' + directory_2)
        shutil.rmtree('/s3-home-folder/' + directory_1)

        with open(f'/s3-home-folder/{directory_2}/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(await object_body(request, f'{directory_2}/{filename}'), b'some-bytes')

    @async_test
    async def test_nested_file_inside_directory_immediate(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop)
        await start()

        directory_1 = str(uuid.uuid4())
        directory_2 = str(uuid.uuid4())
        filename = str(uuid.uuid4())
        os.mkdir(f'/s3-home-folder/{directory_1}')
        os.mkdir(f'/s3-home-folder/{directory_1}/{directory_2}')

        with open(f'/s3-home-folder/{directory_1}/{directory_2}/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        body_bytes = await object_body(request, f'{directory_1}/{directory_2}/{filename}')
        self.assertEqual(body_bytes, b'some-bytes')

    @async_test
    async def test_file_uploaded_after_stop(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        await start()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'\x00' * 10000000)

        await stop()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(await object_body(request, filename), b'\x00' * 10000000)

    @async_test
    async def test_file_closed_half_way_through_with_no_modification(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop)
        await start()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'\x00' * 10000000)

        await asyncio.sleep(0)
        await asyncio.sleep(0)

        with open(f'/s3-home-folder/{filename}', 'a') as file:
            pass

        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(await object_body(request, filename), b'\x00' * 10000000)

    @async_test
    async def test_file_modified_and_closed_half_way_through(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop)
        await start()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'\x00' * 10000000)

        await asyncio.sleep(0)
        await asyncio.sleep(0)

        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'\x01' * 10000000)

        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(await object_body(request, filename), b'\x01' * 10000000)

    @async_test
    async def test_file_changed_half_way_through_no_close_then_close(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop)
        await start()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'\x00' * 10000000)

        await asyncio.sleep(0)
        await asyncio.sleep(0)

        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'\x01' * 10000000)

            await await_upload()

            request, close = get_docker_link_and_minio_compatible_http_pool()
            self.add_async_cleanup(close)

            self.assertEqual(await object_code(request, filename), b'404')

        await await_upload()
        self.assertEqual(await object_body(request, filename), b'\x01' * 10000000)

    @async_test
    async def test_single_small_file_deleted_after_delay(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop)
        await start()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()

        os.remove(f'/s3-home-folder/{filename}')

        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(await object_code(request, filename), b'404')

    @async_test
    async def test_single_small_file_deleted_immediate(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop)
        await start()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        os.remove(f'/s3-home-folder/{filename}')

        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(await object_code(request, filename), b'404')

    @async_test
    async def test_single_small_file_parent_directory_deleted_then_recreated_immediate(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop)
        await start()

        dirname = str(uuid.uuid4())

        os.mkdir(f'/s3-home-folder/{dirname}')
        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{dirname}/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        shutil.rmtree(f'/s3-home-folder/{dirname}')

        os.mkdir(f'/s3-home-folder/{dirname}')
        with open(f'/s3-home-folder/{dirname}/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(await object_body(request, f'{dirname}/{filename}'), b'some-bytes')

    @async_test
    async def test_single_small_file_parent_directory_deleted_then_recreated_after_delay(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop)
        await start()

        dirname = str(uuid.uuid4())

        os.mkdir(f'/s3-home-folder/{dirname}')
        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{dirname}/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()

        shutil.rmtree(f'/s3-home-folder/{dirname}')

        os.mkdir(f'/s3-home-folder/{dirname}')
        with open(f'/s3-home-folder/{dirname}/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(await object_body(request, f'{dirname}/{filename}'), b'some-bytes')

    @async_test
    async def test_file_in_renamed_directory_after_delay(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop)
        await start()

        dirname_1 = str(uuid.uuid4())
        dirname_2 = str(uuid.uuid4())

        os.mkdir(f'/s3-home-folder/{dirname_1}')
        filename = str(uuid.uuid4())

        with open(f'/s3-home-folder/{dirname_1}/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()

        os.rename(f'/s3-home-folder/{dirname_1}', f'/s3-home-folder/{dirname_2}')

        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(await object_code(request, f'{dirname_1}/{filename}'), b'404')
        self.assertEqual(await object_body(request, f'{dirname_2}/{filename}'), b'some-bytes')

    @async_test
    async def test_file_in_renamed_directory_immediate(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop)
        await start()

        dirname_1 = str(uuid.uuid4())
        dirname_2 = str(uuid.uuid4())

        os.mkdir(f'/s3-home-folder/{dirname_1}')
        filename = str(uuid.uuid4())

        with open(f'/s3-home-folder/{dirname_1}/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        os.rename(f'/s3-home-folder/{dirname_1}', f'/s3-home-folder/{dirname_2}')

        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(await object_code(request, f'{dirname_1}/{filename}'), b'404')
        self.assertEqual(await object_body(request, f'{dirname_2}/{filename}'), b'some-bytes')

    @async_test
    async def test_file_in_renamed_nested_directory_after_delay(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop)
        await start()

        dirname_1 = str(uuid.uuid4())
        dirname_2 = str(uuid.uuid4())
        dirname_3 = str(uuid.uuid4())

        os.mkdir(f'/s3-home-folder/{dirname_1}')
        os.mkdir(f'/s3-home-folder/{dirname_1}/{dirname_2}')
        filename = str(uuid.uuid4())

        with open(f'/s3-home-folder/{dirname_1}/{dirname_2}/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()

        os.rename(f'/s3-home-folder/{dirname_1}', f'/s3-home-folder/{dirname_3}')

        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(
            await object_code(request, f'{dirname_1}/{dirname_2}/{filename}'),
            b'404')
        self.assertEqual(
            await object_body(request, f'{dirname_3}/{dirname_2}/{filename}'),
            b'some-bytes')

    @async_test
    async def test_file_created_in_renamed_watched_directory_after_delay(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop)
        await start()

        dirname_1 = str(uuid.uuid4())
        dirname_2 = str(uuid.uuid4())

        os.mkdir(f'/s3-home-folder/{dirname_1}')
        filename = str(uuid.uuid4())

        await await_upload()

        os.rename(f'/s3-home-folder/{dirname_1}', f'/s3-home-folder/{dirname_2}')

        await await_upload()

        with open(f'/s3-home-folder/{dirname_2}/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(await object_code(request, f'{dirname_1}/{filename}'), b'404')
        self.assertEqual(await object_body(request, f'{dirname_2}/{filename}'), b'some-bytes')

    @async_test
    async def test_file_created_in_renamed_watched_directory_immediate(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop)
        await start()

        dirname_1 = str(uuid.uuid4())
        dirname_2 = str(uuid.uuid4())

        os.mkdir(f'/s3-home-folder/{dirname_1}')
        filename = str(uuid.uuid4())

        await await_upload()

        os.rename(f'/s3-home-folder/{dirname_1}', f'/s3-home-folder/{dirname_2}')

        with open(f'/s3-home-folder/{dirname_2}/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(await object_code(request, f'{dirname_1}/{filename}'), b'404')
        self.assertEqual(await object_body(request, f'{dirname_2}/{filename}'), b'some-bytes')

    @async_test
    async def test_file_in_renamed_nested_directory_immediate(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop)
        await start()

        dirname_1 = str(uuid.uuid4())
        dirname_2 = str(uuid.uuid4())
        dirname_3 = str(uuid.uuid4())

        os.mkdir(f'/s3-home-folder/{dirname_1}')
        os.mkdir(f'/s3-home-folder/{dirname_1}/{dirname_2}')
        filename = str(uuid.uuid4())

        with open(f'/s3-home-folder/{dirname_1}/{dirname_2}/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        os.rename(f'/s3-home-folder/{dirname_1}', f'/s3-home-folder/{dirname_3}')

        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(
            await object_code(request, f'{dirname_1}/{dirname_2}/{filename}'),
            b'404')
        self.assertEqual(
            await object_body(request, f'{dirname_3}/{dirname_2}/{filename}'),
            b'some-bytes')

    @async_test
    async def test_file_in_renamed_twice_nested_directory_after_delay(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop)
        await start()

        dirname_1 = str(uuid.uuid4())
        dirname_2 = str(uuid.uuid4())
        dirname_3 = str(uuid.uuid4())
        dirname_4 = str(uuid.uuid4())

        os.mkdir(f'/s3-home-folder/{dirname_1}')
        os.mkdir(f'/s3-home-folder/{dirname_1}/{dirname_2}')
        filename = str(uuid.uuid4())

        with open(f'/s3-home-folder/{dirname_1}/{dirname_2}/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()

        os.rename(f'/s3-home-folder/{dirname_1}', f'/s3-home-folder/{dirname_3}')
        os.rename(f'/s3-home-folder/{dirname_3}', f'/s3-home-folder/{dirname_4}')

        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(
            await object_code(request, f'{dirname_1}/{dirname_2}/{filename}'),
            b'404')
        self.assertEqual(
            await object_code(request, f'{dirname_3}/{dirname_2}/{filename}'),
            b'404')
        self.assertEqual(
            await object_body(request, f'{dirname_4}/{dirname_2}/{filename}'),
            b'some-bytes')

    @async_test
    async def test_file_in_renamed_twice_nested_directory_immediate(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop)
        await start()

        dirname_1 = str(uuid.uuid4())
        dirname_2 = str(uuid.uuid4())
        dirname_3 = str(uuid.uuid4())
        dirname_4 = str(uuid.uuid4())

        os.mkdir(f'/s3-home-folder/{dirname_1}')
        os.mkdir(f'/s3-home-folder/{dirname_1}/{dirname_2}')
        filename = str(uuid.uuid4())

        with open(f'/s3-home-folder/{dirname_1}/{dirname_2}/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        os.rename(f'/s3-home-folder/{dirname_1}', f'/s3-home-folder/{dirname_3}')
        os.rename(f'/s3-home-folder/{dirname_3}', f'/s3-home-folder/{dirname_4}')

        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(
            await object_code(request, f'{dirname_1}/{dirname_2}/{filename}'),
            b'404')
        self.assertEqual(
            await object_code(request, f'{dirname_3}/{dirname_2}/{filename}'),
            b'404')
        self.assertEqual(
            await object_body(request, f'{dirname_4}/{dirname_2}/{filename}'),
            b'some-bytes')

    @async_test
    async def test_file_rename_immediate(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop)
        await start()

        filename_1 = str(uuid.uuid4())
        filename_2 = str(uuid.uuid4())

        with open(f'/s3-home-folder/{filename_1}', 'wb') as file:
            file.write(b'some-bytes')

        os.rename(f'/s3-home-folder/{filename_1}', f'/s3-home-folder/{filename_2}')

        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(await object_code(request, f'{filename_1}'), b'404')
        self.assertEqual(await object_body(request, f'{filename_2}'), b'some-bytes')

    @async_test
    async def test_file_rename_after_delay(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop)
        await start()

        filename_1 = str(uuid.uuid4())
        filename_2 = str(uuid.uuid4())

        with open(f'/s3-home-folder/{filename_1}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()

        os.rename(f'/s3-home-folder/{filename_1}', f'/s3-home-folder/{filename_2}')

        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(await object_code(request, f'{filename_1}'), b'404')
        self.assertEqual(await object_body(request, f'{filename_2}'), b'some-bytes')

    @async_test
    async def test_file_delete_immediate(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop)
        await start()

        dirname = str(uuid.uuid4())
        os.mkdir(f'/s3-home-folder/{dirname}')
        filename = str(uuid.uuid4())

        with open(f'/s3-home-folder/{dirname}/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        os.remove(f'/s3-home-folder/{dirname}/{filename}')

        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(await object_code(request, f'{dirname}/{filename}'), b'404')

    @async_test
    async def test_file_delete_after_delay(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop)
        await start()

        dirname = str(uuid.uuid4())
        os.mkdir(f'/s3-home-folder/{dirname}')
        filename = str(uuid.uuid4())

        with open(f'/s3-home-folder/{dirname}/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()

        os.remove(f'/s3-home-folder/{dirname}/{filename}')

        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(await object_code(request, f'{dirname}/{filename}'), b'404')

    @async_test
    async def test_many_files_delete_after_delay(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop)
        await start()

        dirname = str(uuid.uuid4())
        os.mkdir(f'/s3-home-folder/{dirname}')

        filenames = [
            str(uuid.uuid4()) for _ in range(0, 100)
        ]

        for filename in filenames:
            with open(f'/s3-home-folder/{dirname}/{filename}', 'wb') as file:
                file.write(b'some-bytes')

        await await_upload()

        for filename in filenames:
            os.remove(f'/s3-home-folder/{dirname}/{filename}')

        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        for filename in filenames:
            self.assertEqual(await object_code(request, f'{dirname}/{filename}'), b'404')

    @async_test
    async def test_file_named_as_flush_uploaded_with_others(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.add_async_cleanup(stop)
        await start()

        dirname = str(uuid.uuid4())
        filename_1 = '.__mobius3__some_file'
        filename_2 = str(uuid.uuid4())

        os.mkdir(f'/s3-home-folder/{dirname}')

        await await_upload()

        with open(f'/s3-home-folder/{dirname}/{filename_1}', 'wb') as file:
            file.write(b'some-bytes')

        with open(f'/s3-home-folder/{dirname}/{filename_2}', 'wb') as file:
            file.write(b'more-bytes')

        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)
        self.assertEqual(await object_body(request, f'{dirname}/{filename_1}'), b'some-bytes')
        self.assertEqual(await object_body(request, f'{dirname}/{filename_2}'), b'more-bytes')

    @async_test
    async def test_file_created_after_overflow(self):

        max_queued_events = 16384

        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        await start()

        filename_1 = str(uuid.uuid4())
        filename_2 = str(uuid.uuid4())

        for _ in range(0, max_queued_events):
            with open(f'/s3-home-folder/{filename_1}', 'wb') as file:
                file.write(b'some-bytes')
            os.remove(f'/s3-home-folder/{filename_1}')

        with open(f'/s3-home-folder/{filename_2}', 'wb') as file:
            file.write(b'more-bytes')

        await stop()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(await object_body(request, filename_2), b'more-bytes')


class TestEndToEnd(unittest.TestCase):

    def add_async_cleanup(self, coroutine, *args):
        loop = asyncio.get_event_loop()
        self.addCleanup(loop.run_until_complete, coroutine(*args))

    @async_test
    async def test_console_script(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        install_mobius3 = await asyncio.create_subprocess_exec('python3', 'setup.py', 'develop')
        self.add_async_cleanup(terminate, install_mobius3)
        await install_mobius3.wait()

        mobius3_process = await asyncio.create_subprocess_exec(
            'mobius3', '/s3-home-folder', f'https://minio:9000/my-bucket/', 'us-east-1',
            '--disable-ssl-verification', '--disable-0x20-dns-encoding',
            env=os.environ, stdout=asyncio.subprocess.PIPE,
        )
        self.add_async_cleanup(terminate, mobius3_process)

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()
        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(await object_body(request, filename), b'some-bytes')

        await await_upload()

    @async_test
    async def test_direct_script_delay(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        mobius3_process = await asyncio.create_subprocess_exec(
            sys.executable, '-m', 'mobius3',
            '/s3-home-folder', f'https://minio:9000/my-bucket/', 'us-east-1',
            '--disable-ssl-verification', '--disable-0x20-dns-encoding',
            env=os.environ, stdout=asyncio.subprocess.PIPE,
        )
        self.add_async_cleanup(terminate, mobius3_process)

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()
        await await_upload()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(await object_body(request, filename), b'some-bytes')

        await await_upload()

    @async_test
    async def test_direct_script_after_stop(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        mobius3_process = await asyncio.create_subprocess_exec(
            sys.executable, '-m', 'mobius3',
            '/s3-home-folder', f'https://minio:9000/my-bucket/', 'us-east-1',
            '--disable-ssl-verification', '--disable-0x20-dns-encoding',
            env=os.environ, stdout=asyncio.subprocess.PIPE,
        )
        self.add_async_cleanup(terminate, mobius3_process)

        await await_upload()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        mobius3_process.terminate()
        await mobius3_process.wait()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(await object_body(request, filename), b'some-bytes')

        await await_upload()

    @async_test
    async def test_direct_script_without_prefix_after_stop(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        mobius3_process = await asyncio.create_subprocess_exec(
            sys.executable, '-m', 'mobius3',
            '/s3-home-folder', f'https://minio:9000/my-bucket/', 'us-east-1',
            '--disable-ssl-verification', '--disable-0x20-dns-encoding',
            env=os.environ, stdout=asyncio.subprocess.PIPE,
        )
        self.add_async_cleanup(terminate, mobius3_process)

        await await_upload()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        mobius3_process.terminate()
        await mobius3_process.wait()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(await object_body(request, filename), b'some-bytes')

        await await_upload()

    @async_test
    async def test_direct_script_with_prefix_after_stop(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.add_async_cleanup(delete_bucket_dir)

        mobius3_process = await asyncio.create_subprocess_exec(
            sys.executable, '-m', 'mobius3',
            '/s3-home-folder', f'https://minio:9000/my-bucket/', 'us-east-1',
            '--prefix', 'my-folder/',
            '--disable-ssl-verification', '--disable-0x20-dns-encoding',
            env=os.environ, stdout=asyncio.subprocess.PIPE,
        )
        self.add_async_cleanup(terminate, mobius3_process)

        await await_upload()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        mobius3_process.terminate()
        await mobius3_process.wait()

        request, close = get_docker_link_and_minio_compatible_http_pool()
        self.add_async_cleanup(close)

        self.assertEqual(await object_body(request, f'my-folder/{filename}'), b'some-bytes')

        await await_upload()


def create_directory(path):
    async def delete_dir():
        try:
            shutil.rmtree(path)
        except OSError:
            pass

    try:
        os.mkdir(path)
    except FileExistsError:
        pass

    return delete_dir


def get_docker_link_and_minio_compatible_http_pool():
    async def transform_fqdn(fqdn):
        return fqdn

    ssl_context = ssl.SSLContext()
    ssl_context.verify_mode = ssl.CERT_NONE

    return Pool(
        # 0x20 encoding does not appear to work with linked containers
        get_dns_resolver=lambda **kwargs: Resolver(**{
            'transform_fqdn': transform_fqdn,
            **kwargs,
        }),
        # We use self-signed certs locally
        get_ssl_context=lambda: ssl_context,
    )


async def terminate(process):
    try:
        process.terminate()
    except ProcessLookupError:
        pass
    else:
        await process.wait()


def syncer_for(path, prefix='',
               local_modification_persistance=120, download_interval=60,
               exclude_remote='^$',
               exclude_local='^$',):
    return Syncer(
        path, 'https://minio:9000/my-bucket/', 'us-east-1',
        get_pool=get_docker_link_and_minio_compatible_http_pool,
        prefix=prefix,
        local_modification_persistance=local_modification_persistance,
        download_interval=download_interval,
        exclude_remote=exclude_remote,
        exclude_local=exclude_local,
    )


async def await_upload():
    await asyncio.sleep(1)


async def object_body(request, key):
    _, _, body = await object_triple(request, key)
    body_bytes = await buffered(body)
    return body_bytes


async def object_code(request, key):
    code, _, body = await object_triple(request, key)
    await buffered(body)
    return code


async def get_credentials_from_environment():
    return os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'], ()


async def object_triple(request, key):
    signed_request = signed(
        request, credentials=get_credentials_from_environment,
        service='s3', region='us-east-1',
    )
    return await signed_request(b'GET', f'https://minio:9000/my-bucket/{key}')


async def delete_object(request, key):
    signed_request = signed(
        request, credentials=get_credentials_from_environment,
        service='s3', region='us-east-1',
    )
    return await signed_request(b'DELETE', f'https://minio:9000/my-bucket/{key}')


async def put_body(request, key, body):
    signed_request = signed(
        request, credentials=get_credentials_from_environment,
        service='s3', region='us-east-1',
    )
    return await signed_request(b'PUT', f'https://minio:9000/my-bucket/{key}',
                                headers=((b'content-length', str(len(body)).encode()),),
                                body=streamed(body))
