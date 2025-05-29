import asyncio
from datetime import (
    datetime,
)
import hashlib
import json
import os
import re
import shutil
import ssl
import sys
import unittest
import urllib.parse
import uuid

from aiohttp import (
    web,
)
from mobius3 import (
    AWSAuth,
    Pool,
    Syncer,
    empty_async_iterator,
    streamed,
    buffered,
)


class TestIntegration(unittest.IsolatedAsyncioTestCase):

    async def test_download_file_at_start_then_upload(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        filename_1 = str(uuid.uuid4())
        response = await put_body(client, f'prefix/{filename_1}', b'some-bytes')
        self.assertEqual(response.status_code, 200)

        date = response.headers['date']
        date_ts = datetime.strptime(date, '%a, %d %b %Y %H:%M:%S %Z').timestamp()

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

        self.assertEqual(await object_body(client, f'prefix/{filename_2}'), b'more-bytes')

    async def test_download_nested_files_at_start(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        filename_1 = str(uuid.uuid4())
        filename_2 = str(uuid.uuid4())
        directory = str(uuid.uuid4())
        response = await put_body(client, f'prefix/{directory}/{filename_1}', b'some-bytes')
        self.assertEqual(response.status_code, 200)
        response = await put_body(client, f'prefix/{directory}/{filename_2}', b'more-bytes')
        self.assertEqual(response.status_code, 200)

        start, stop = syncer_for('/s3-home-folder', prefix='prefix/')
        self.addAsyncCleanup(stop)

        await start()

        with open(f'/s3-home-folder/{directory}/{filename_1}', 'rb') as file:
            body_bytes = file.read()
        self.assertEqual(body_bytes, b'some-bytes')
        with open(f'/s3-home-folder/{directory}/{filename_2}', 'rb') as file:
            body_bytes = file.read()
        self.assertEqual(body_bytes, b'more-bytes')

    async def test_exclude_local_after_start(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for(
            '/s3-home-folder',
            exclude_local=r'.*to-exclude.*',
            local_modification_persistance=1,
            download_interval=1,
        )
        self.addAsyncCleanup(stop)
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

        await asyncio.sleep(2)

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_body(client, filename_1), b'some-bytes-a')
        self.assertEqual(await object_code(client, filename_2), 404)
        self.assertEqual(await object_code(client, f'{dirname_1}/{filename_3}'), 404)

        # Minio seems to return a 200 for all folders, but this _should_ assertEqual for S3 proper
        # self.assertEqual(await object_code(request, f'{dirname_1}/'), 404)

        self.assertTrue(os.path.exists(f'/s3-home-folder/{filename_2}'))

    async def test_exclude_remote_at_start(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        filename_1 = str(uuid.uuid4())
        filename_2 = str(uuid.uuid4())
        directory = str(uuid.uuid4())
        response = await put_body(client, f'prefix/{filename_1}/.checkpoints/check_1',
                                  b'some-inner-bytes')
        self.assertEqual(response.status_code, 200)
        response = await put_body(client, f'prefix/{directory}/{filename_2}', b'more-bytes')
        self.assertEqual(response.status_code, 200)

        start, stop = syncer_for('/s3-home-folder', prefix='prefix/',
                                 exclude_remote=re.compile(r'.*\.checkpoints/.*'))
        self.addAsyncCleanup(stop)

        await start()

        self.assertFalse(os.path.exists(f'/s3-home-folder/{filename_1}/.checkpoints/check_1'))

        with open(f'/s3-home-folder/{directory}/{filename_2}', 'rb') as file:
            body_bytes = file.read()
        self.assertEqual(body_bytes, b'more-bytes')

    async def test_download_file_after_start(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        start, stop = syncer_for(
            '/s3-home-folder', prefix='prefix/',
            download_interval=1)
        self.addAsyncCleanup(stop)

        await start()

        filename_1 = str(uuid.uuid4())
        response = await put_body(client, f'prefix/{filename_1}', b'some-bytes')
        self.assertEqual(response.status_code, 200)

        date = response.headers['date']
        date_ts = datetime.strptime(date, '%a, %d %b %Y %H:%M:%S %Z').timestamp()

        await asyncio.sleep(2)

        with open(f'/s3-home-folder/{filename_1}', 'rb') as file:
            body_bytes = file.read()

        self.assertEqual(body_bytes, b'some-bytes')
        self.assertEqual(date_ts, os.path.getmtime(f'/s3-home-folder/{filename_1}'))

        await asyncio.sleep(2)

        # Ensure the file isn't re-downloaded, or at least if it is, it has
        # the correct mtime
        self.assertEqual(date_ts, os.path.getmtime(f'/s3-home-folder/{filename_1}'))

    async def test_download_file_after_start_with_existing_prefix_object(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        # We want to ensure nothing bad happens if the remote prefix object
        # exists [essentially, we ignore this object]
        delete_prefix_dir = create_directory('/test-data/my-bucket/prefix')
        self.addAsyncCleanup(delete_prefix_dir)

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        start, stop = syncer_for(
            '/s3-home-folder', prefix='prefix/',
            download_interval=1)
        self.addAsyncCleanup(stop)

        await start()

        filename_1 = str(uuid.uuid4())
        response = await put_body(client, f'prefix/{filename_1}', b'some-bytes')
        self.assertEqual(response.status_code, 200)

        date = response.headers['date']
        date_ts = datetime.strptime(date, '%a, %d %b %Y %H:%M:%S %Z').timestamp()

        await asyncio.sleep(2)

        with open(f'/s3-home-folder/{filename_1}', 'rb') as file:
            body_bytes = file.read()

        self.assertEqual(body_bytes, b'some-bytes')
        self.assertEqual(date_ts, os.path.getmtime(f'/s3-home-folder/{filename_1}'))

        await asyncio.sleep(2)

        # Ensure the file isn't re-downloaded, or at least if it is, it has
        # the correct mtime
        self.assertEqual(date_ts, os.path.getmtime(f'/s3-home-folder/{filename_1}'))

    async def test_download_nested_file_after_start(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        start, stop = syncer_for(
            '/s3-home-folder', prefix='prefix/',
            download_interval=1)
        self.addAsyncCleanup(stop)

        await start()

        dirname_1 = str(uuid.uuid4())
        filename_1 = str(uuid.uuid4())
        response = await put_body(
            client, f'prefix/{dirname_1}/{filename_1}',
            b'some-bytes')
        self.assertEqual(response.status_code, 200)
        date = response.headers['date']

        await asyncio.sleep(2)

        # Ensure the modified date stays the same to check that a second upload
        # has not occured
        response = await object_response(client, f'prefix/{dirname_1}/{filename_1}')
        self.assertEqual(response.status_code, 200)
        modified = response.headers['last-modified']
        self.assertEqual(modified, date)

        with open(f'/s3-home-folder/{dirname_1}/{filename_1}', 'rb') as file:
            body_bytes = file.read()

        self.assertEqual(body_bytes, b'some-bytes')

        dirname_2 = str(uuid.uuid4())
        os.rename(f'/s3-home-folder/{dirname_1}', f'/s3-home-folder/{dirname_2}')

        await asyncio.sleep(2)

        self.assertEqual(await object_code(client, f'prefix/{dirname_1}/{filename_1}'), 404)
        self.assertEqual(await object_body(
            client, f'prefix/{dirname_2}/{filename_1}'), b'some-bytes')

    async def test_download_directory_after_start(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)

        dirname_1 = str(uuid.uuid4())
        dirname_2 = str(uuid.uuid4())
        dirname_3 = str(uuid.uuid4())

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
                    <Key>{dirname_1}/some-file</Key>
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

        async def handle_file(_):
            return web.Response(status=200, headers={
                'last-modified': 'Fri, 10 May 2019 06:53:17 GMT',
                'etag': '"fba9dede5f27731c9771645a39863328"',
            }, body=b'some-bytes')

        app = web.Application()
        app.add_routes([
            web.get(f'/my-bucket/', handle_list),
            web.get(f'/my-bucket/{dirname_1}/', handle_dir),
            web.get(f'/my-bucket/{dirname_1}/some-file', handle_file),
            web.get(f'/my-bucket/{dirname_2}/', handle_dir),
            web.get(f'/my-bucket/{dirname_2}/{dirname_3}/', handle_dir),
        ])
        runner = web.AppRunner(app)
        await runner.setup()
        self.addAsyncCleanup(runner.cleanup)
        site = web.TCPSite(runner, '0.0.0.0', 8080)
        await site.start()

        start, stop = Syncer(
            '/s3-home-folder', 'my-bucket', 'http://localhost:8080/{}/', 'us-east-1',
        )
        self.addAsyncCleanup(stop)
        await start()

        with open(f'/s3-home-folder/{dirname_1}/some-file', 'rb') as file:
            body_bytes = file.read()
        self.assertEqual(body_bytes, b'some-bytes')

        self.assertTrue(os.path.isdir(f'/s3-home-folder/{dirname_1}'))
        self.assertTrue(os.path.isdir(f'/s3-home-folder/{dirname_2}/{dirname_3}'))
        self.assertEqual(os.path.getmtime(f'/s3-home-folder/{dirname_2}/{dirname_3}'),
                         1557471197.0)

    async def test_download_directory_in_prefix_after_start(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)

        dirname_1 = str(uuid.uuid4())
        dirname_2 = str(uuid.uuid4())
        dirname_3 = str(uuid.uuid4())

        # minio does not support keys with trailing slashes, so we fire up our
        # own mock S3
        async def handle_list(_):
            body = f'''<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Contents>
                    <Key>prefix/</Key>
                    <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                </Contents>
                <Contents>
                    <Key>prefix/{dirname_1}/</Key>
                    <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                </Contents>
                <Contents>
                    <Key>prefix/{dirname_1}/some-file</Key>
                    <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                </Contents>
                <Contents>
                    <Key>prefix/{dirname_2}/{dirname_3}/</Key>
                    <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                </Contents>
            </ListBucketResult>'''.encode()
            return web.Response(status=200, body=body)

        async def handle_dir(_):
            return web.Response(status=200, headers={
                'last-modified': 'Fri, 10 May 2019 06:53:17 GMT',
                'etag': '"fba9dede5f27731c9771645a39863328"',
            }, body=b'')

        async def handle_file(_):
            return web.Response(status=200, headers={
                'last-modified': 'Fri, 10 May 2019 06:53:17 GMT',
                'etag': '"fba9dede5f27731c9771645a39863328"',
            }, body=b'some-bytes')

        app = web.Application()
        app.add_routes([
            web.get(f'/my-bucket/', handle_list),
            web.get(f'/my-bucket/prefix/', handle_dir),
            web.get(f'/my-bucket/prefix/{dirname_1}/', handle_dir),
            web.get(f'/my-bucket/prefix/{dirname_1}/some-file', handle_file),
            web.get(f'/my-bucket/prefix/{dirname_2}/', handle_dir),
            web.get(f'/my-bucket/prefix/{dirname_2}/{dirname_3}/', handle_dir),
        ])
        runner = web.AppRunner(app)
        await runner.setup()
        self.addAsyncCleanup(runner.cleanup)
        site = web.TCPSite(runner, '0.0.0.0', 8080)
        await site.start()

        start, stop = Syncer(
            '/s3-home-folder', 'my-bucket', 'http://localhost:8080/{}/', 'us-east-1',
            prefix='prefix/'
        )
        self.addAsyncCleanup(stop)
        await start()

        with open(f'/s3-home-folder/{dirname_1}/some-file', 'rb') as file:
            body_bytes = file.read()
        self.assertEqual(body_bytes, b'some-bytes')

        self.assertTrue(os.path.isdir(f'/s3-home-folder/{dirname_1}'))
        self.assertTrue(os.path.isdir(f'/s3-home-folder/{dirname_2}/{dirname_3}'))
        self.assertEqual(os.path.getmtime(f'/s3-home-folder/{dirname_2}/{dirname_3}'),
                         1557471197.0)

    async def test_download_in_nested_directory_at_start(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)

        dirname_1 = str(uuid.uuid4())
        dirname_2 = str(uuid.uuid4())

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
                    <Key>{dirname_1}/{dirname_2}/</Key>
                    <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                </Contents>
                <Contents>
                    <Key>{dirname_1}/{dirname_2}/some-file</Key>
                    <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                </Contents>
            </ListBucketResult>'''.encode()
            return web.Response(status=200, body=body)

        async def handle_dir(_):
            return web.Response(status=200, headers={
                'last-modified': 'Fri, 10 May 2019 06:53:17 GMT',
                'etag': '"fba9dede5f27731c9771645a39863328"',
            }, body=b'')

        async def handle_file(_):
            return web.Response(status=200, headers={
                'last-modified': 'Fri, 10 May 2019 06:53:17 GMT',
                'etag': '"fba9dede5f27731c9771645a39863328"',
            }, body=b'some-bytes')

        app = web.Application()
        app.add_routes([
            web.get(f'/my-bucket/', handle_list),
            web.get(f'/my-bucket/{dirname_1}/', handle_dir),
            web.get(f'/my-bucket/{dirname_1}/{dirname_2}/', handle_dir),
            web.get(f'/my-bucket/{dirname_1}/{dirname_2}/some-file', handle_file),
        ])
        runner = web.AppRunner(app)
        await runner.setup()
        self.addAsyncCleanup(runner.cleanup)
        site = web.TCPSite(runner, '0.0.0.0', 8080)
        await site.start()

        start, stop = Syncer(
            '/s3-home-folder', 'my-bucket', 'http://localhost:8080/{}/', 'us-east-1',
        )
        self.addAsyncCleanup(stop)
        await start()

        with open(f'/s3-home-folder/{dirname_1}/{dirname_2}/some-file', 'rb') as file:
            body_bytes = file.read()
        self.assertEqual(body_bytes, b'some-bytes')

    async def test_delete_downloaded_directory(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)

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
        ])
        runner = web.AppRunner(app)
        await runner.setup()
        self.addAsyncCleanup(runner.cleanup)
        site = web.TCPSite(runner, '0.0.0.0', 8080)
        await site.start()

        start, stop = Syncer(
            '/s3-home-folder', 'my-bucket', 'http://localhost:8080/{}/', 'us-east-1',
            local_modification_persistance=1,
            download_interval=1,
        )
        self.addAsyncCleanup(stop)
        await start()

        self.assertTrue(os.path.isdir(f'/s3-home-folder/{dirname_1}/{dirname_2}'))

        await asyncio.sleep(1)

        # Simulate files having been deleted on S3
        list_body = without_dirs
        get_code = 404
        get_headers = {}

        await asyncio.sleep(2)

        self.assertFalse(os.path.exists(f'/s3-home-folder/{dirname_1}'))

    async def test_delete_existing_file_after_initial_download(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)

        # This _should_ end up deleted, but not until we've saved the remote
        # files locally
        filename_local = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename_local}', 'wb') as file:
            file.write(b'some-local-bytes')

        # Ensure the file has not been modified within local_modification_persistance
        await asyncio.sleep(1)

        filename_remote = str(uuid.uuid4())

        async def handle_list(_):
            return web.Response(status=200, body=f'''<?xml version="1.0" encoding="UTF-8"?>
                <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                    <Contents>
                        <Key>{filename_remote}</Key>
                        <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
                    </Contents>
                </ListBucketResult>'''.encode()
                                )

        async def handle_file(_):
            await asyncio.sleep(7)
            return web.Response(status=200, headers={
                'last-modified': 'Fri, 10 May 2019 06:53:17 GMT',
                'etag': '"fba9dede5f27731c9771645a39863328"',
            }, body=b'some-remote-bytes')

        app = web.Application()
        app.add_routes([
            web.get(f'/my-bucket/', handle_list),
            web.get(f'/my-bucket/{filename_remote}', handle_file),
        ])
        runner = web.AppRunner(app)
        await runner.setup()
        self.addAsyncCleanup(runner.cleanup)
        site = web.TCPSite(runner, '0.0.0.0', 8080)
        await site.start()

        start, stop = Syncer(
            '/s3-home-folder', 'my-bucket', 'http://localhost:8080/{}/', 'us-east-1',
            local_modification_persistance=1,
        )
        self.addAsyncCleanup(stop)
        asyncio.create_task(start())

        # We have a slow initial download, during which the existing file
        # that will eventually be deleted, should remain...
        for _ in range(0, 4):
            self.assertTrue(os.path.exists(f'/s3-home-folder/{filename_local}'))
            self.assertFalse(os.path.exists(f'/s3-home-folder/{filename_remote}'))
            await asyncio.sleep(1)

        await asyncio.sleep(4)

        # And then after the download, the existing file should be deleted
        self.assertFalse(os.path.exists(f'/s3-home-folder/{filename_local}'))
        self.assertTrue(os.path.exists(f'/s3-home-folder/{filename_remote}'))

    async def test_nested_delete_downloaded_directory(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)

        dirname_1 = str(uuid.uuid4())
        dirname_2 = str(uuid.uuid4())
        dirname_3 = str(uuid.uuid4())
        dirname_4 = str(uuid.uuid4())
        dirname_5 = str(uuid.uuid4())
        dirname_6 = str(uuid.uuid4())
        dirname_7 = str(uuid.uuid4())
        dirname_8 = str(uuid.uuid4())

        with_dirs = f'''<?xml version="1.0" encoding="UTF-8"?>
            <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <Contents>
                    <Key>{dirname_1}/{dirname_2}/{dirname_3}/{dirname_4}/{dirname_5}/{dirname_6}/{dirname_7}/{dirname_8}/</Key>
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
            web.get(f'/my-bucket/{dirname_1}/{dirname_2}/{dirname_3}/', handle_dir),
            web.get(f'/my-bucket/{dirname_1}/{dirname_2}/{dirname_3}/{dirname_4}/', handle_dir),
            web.get(
                f'/my-bucket/{dirname_1}/{dirname_2}/{dirname_3}/'
                f'{dirname_4}/{dirname_5}/', handle_dir),
            web.get(
                f'/my-bucket/{dirname_1}/{dirname_2}/{dirname_3}/'
                f'{dirname_4}/{dirname_5}/{dirname_6}/', handle_dir),
            web.get(
                f'/my-bucket/{dirname_1}/{dirname_2}/{dirname_3}/'
                f'{dirname_4}/{dirname_5}/{dirname_6}/{dirname_7}/', handle_dir),
            web.get(
                f'/my-bucket/{dirname_1}/{dirname_2}/{dirname_3}/'
                f'{dirname_4}/{dirname_5}/{dirname_6}/{dirname_7}/{dirname_8}/', handle_dir),
        ])
        runner = web.AppRunner(app)
        await runner.setup()
        self.addAsyncCleanup(runner.cleanup)
        site = web.TCPSite(runner, '0.0.0.0', 8080)
        await site.start()

        start, stop = Syncer(
            '/s3-home-folder', 'my-bucket', 'http://localhost:8080/{}/', 'us-east-1',
            local_modification_persistance=1,
            download_interval=1,
        )
        self.addAsyncCleanup(stop)
        await start()

        self.assertTrue(os.path.isdir(
            f'/s3-home-folder/{dirname_1}/{dirname_2}/{dirname_3}/{dirname_4}/'
            f'{dirname_5}/{dirname_6}/{dirname_7}/{dirname_8}/'))

        await asyncio.sleep(1)

        # Simulate files having been deleted on S3
        list_body = without_dirs
        get_code = 404
        get_headers = {}

        await asyncio.sleep(2)

        self.assertFalse(os.path.exists(f'/s3-home-folder/{dirname_1}'))

    async def test_download_file_not_done_during_local_persistance(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        start, stop = syncer_for(
            '/s3-home-folder', prefix='prefix/',
            local_modification_persistance=4,
            download_interval=1,
        )
        self.addAsyncCleanup(stop)

        await start()

        filename_1 = str(uuid.uuid4())

        with open(f'/s3-home-folder/{filename_1}', 'wb') as file:
            file.write(b'some-bytes')

        # Wait for the transfer to take place
        await asyncio.sleep(1)

        # The remote file is overridden
        response = await put_body(client, f'prefix/{filename_1}', b'some-remote-bytes')
        self.assertEqual(response.status_code, 200)

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

    async def test_download_file_repeated_remote_changes(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        start, stop = syncer_for(
            '/s3-home-folder', prefix='prefix/',
            download_interval=1,
            local_modification_persistance=1
        )
        self.addAsyncCleanup(stop)

        await start()

        filename_1 = str(uuid.uuid4())

        response = await put_body(client, f'prefix/{filename_1}', b'some-remote-bytes-a')
        self.assertEqual(response.status_code, 200)

        # Ensure that the local file isoverridden
        await asyncio.sleep(2)

        with open(f'/s3-home-folder/{filename_1}', 'rb') as file:
            body_bytes = file.read()
        self.assertEqual(body_bytes, b'some-remote-bytes-a')

        response = await put_body(client, f'prefix/{filename_1}', b'some-remote-bytes-b')
        self.assertEqual(response.status_code, 200)

        # Ensure that the local file is overritten
        await asyncio.sleep(2)

        with open(f'/s3-home-folder/{filename_1}', 'rb') as file:
            body_bytes = file.read()
        self.assertEqual(body_bytes, b'some-remote-bytes-b')

    async def test_single_small_file_uploaded(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
        await start()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_body(client, filename), b'some-bytes')

    async def test_hard_linked_file_uploaded_if_matches_upload_on_create(self):
        # This is to simulate how git creates pack files
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder', upload_on_create=r'.*_link')
        self.addAsyncCleanup(stop)
        await start()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        os.link(f'/s3-home-folder/{filename}', f'/s3-home-folder/{filename}_link')
        os.link(f'/s3-home-folder/{filename}', f'/s3-home-folder/{filename}_nomatchlink')
        os.remove(f'/s3-home-folder/{filename}')

        await await_upload()

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_body(client, f'{filename}_link'), b'some-bytes')
        self.assertEqual(await object_code(client, f'{filename}_nomatchlink'), 404)

    async def test_symlinks_uploaded_on_create(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
        await start()

        filename = str(uuid.uuid4())
        filename_2 = str(uuid.uuid4())
        dir_name = str(uuid.uuid4())

        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'some-bytes')
        os.mkdir(f'/s3-home-folder/{dir_name}')
        with open(f'/s3-home-folder/{dir_name}/{filename_2}', 'wb') as file:
            file.write(b'some-bytes')

        # Symlink to existing file
        os.symlink(f'/s3-home-folder/{filename}', '/s3-home-folder/file_link')
        # Symlink to existing directory
        os.symlink(f'/s3-home-folder/{dir_name}', '/s3-home-folder/dir_link')
        # Symlink to file in directory
        os.symlink(f'/s3-home-folder/{dir_name}/{filename_2}', '/s3-home-folder/file_in_dir_link')
        # Symlink to symlinked directory
        os.symlink('/s3-home-folder/dir_link', '/s3-home-folder/symlinked_dir_link')
        # Symlink to file in symlinked directory
        os.symlink(f'/s3-home-folder/symlinked_dir_link/{filename_2}', '/s3-home-folder/file_in_symlinked_dir_link')

        # Symlink to non existent file
        os.symlink('/s3-home-folder/bad_file', '/s3-home-folder/bad_file_link')
        # Symlink to non existent directory
        os.symlink('/s3-home-folder/bad_dir', '/s3-home-folder/bad_dir_link')
        # Symlink loop
        os.symlink('/s3-home-folder/loop_link', '/s3-home-folder/loop_link')
        # Symlink to unicode filename
        os.symlink('/s3-home-folder/🍰', '/s3-home-folder/cake_link')

        await asyncio.sleep(1)

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_body(client, 'file_link'), f'/s3-home-folder/{filename}'.encode('utf-8'))
        self.assertEqual(await object_body(client, 'dir_link'), f'/s3-home-folder/{dir_name}'.encode('utf-8'))
        self.assertEqual(await object_body(client, 'file_in_dir_link'), f'/s3-home-folder/{dir_name}/{filename_2}'.encode('utf-8'))
        self.assertEqual(await object_body(client, 'symlinked_dir_link'), '/s3-home-folder/dir_link'.encode('utf-8'))
        self.assertEqual(await object_body(client, 'file_in_symlinked_dir_link'), f'/s3-home-folder/symlinked_dir_link/{filename_2}'.encode('utf-8'))
        self.assertEqual(await object_body(client, 'bad_file_link'), '/s3-home-folder/bad_file'.encode('utf-8'))
        self.assertEqual(await object_body(client, 'bad_dir_link'), '/s3-home-folder/bad_dir'.encode('utf-8'))
        self.assertEqual(await object_body(client, 'loop_link'), '/s3-home-folder/loop_link'.encode('utf-8'))
        self.assertEqual(await object_body(client, 'cake_link'), '/s3-home-folder/🍰'.encode('utf-8'))

    async def test_symlinks_are_preserved(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop_a = syncer_for('/s3-home-folder')
        stopped = False

        async def stop_once():
            nonlocal stopped
            if stopped:
                return
            stopped = True
            await stop_a()
        self.addAsyncCleanup(stop_once)
        await start()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'some-bytes')
        os.symlink(f'/s3-home-folder/{filename}', f'/s3-home-folder/{filename}_link')
        mtime_1 = os.path.getmtime(f'/s3-home-folder/{filename}_link')

        await await_upload()

        await stop_once()
        await delete_dir()

        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)

        await asyncio.sleep(1)

        start, stop_b = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop_b)
        await start()

        await asyncio.sleep(1)

        with open(f'/s3-home-folder/{filename}_link', 'rb') as file:
            body_bytes = file.read()
        points_to = os.readlink(f'/s3-home-folder/{filename}_link')
        mtime_2 = os.path.getmtime(f'/s3-home-folder/{filename}_link')

        self.assertEqual(body_bytes, b'some-bytes')
        self.assertTrue(os.path.islink(f'/s3-home-folder/{filename}_link'))
        self.assertEqual(points_to, f'/s3-home-folder/{filename}')
        self.assertEqual(mtime_1, mtime_2)

    async def test_directory_uploaded_after_start_then_manipulated(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)

        dirname_1 = str(uuid.uuid4())
        dirname_2 = str(uuid.uuid4())
        dirname_3 = str(uuid.uuid4())
        dirname_4 = str(uuid.uuid4())

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
        self.addAsyncCleanup(runner.cleanup)
        site = web.TCPSite(runner, '0.0.0.0', 8080)
        await site.start()

        start, stop = Syncer(
            '/s3-home-folder', 'my-bucket', 'http://localhost:8080/{}/', 'us-east-1',
        )
        self.addAsyncCleanup(stop)
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

    async def test_single_medium_file_uploaded(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
        await start()

        filename = str(uuid.uuid4())
        contents = str(uuid.uuid4()).encode() * 100000
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(contents)

        await await_upload()

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_body(client, filename), contents)

    async def test_larger_numbers_of_files(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder',
                                 local_modification_persistance=2, download_interval=1)
        self.addAsyncCleanup(stop)
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

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)
        for filename, contents in filenames_contents:
            self.assertEqual(await object_body(client, filename), contents)

        last_filename = filenames_contents[-1][0]
        await put_body(client, last_filename, b'some-bytes')

        await asyncio.sleep(7)

        with open(f'/s3-home-folder/{last_filename}', 'rb') as file:
            body_bytes = file.read()
        self.assertEqual(body_bytes, b'some-bytes')

    async def test_files_deleted_remotely(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder',
                                 local_modification_persistance=1, download_interval=1)
        self.addAsyncCleanup(stop)
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

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)
        await delete_object(client, f'{last_filename}')

        await asyncio.sleep(2)

        for filename, content in filenames_contents[:-1]:
            with open(f'/s3-home-folder/{filename}', 'rb') as file:
                body_bytes = file.read()
            self.assertEqual(body_bytes, content)

        self.assertFalse(os.path.exists(f'/s3-home-folder/{last_filename}'))

    async def test_file_upload_preserves_mtime(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop_a = syncer_for('/s3-home-folder')
        stopped = False

        async def stop_once():
            nonlocal stopped
            if stopped:
                return
            stopped = True
            await stop_a()
        self.addAsyncCleanup(stop_once)
        await start()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        mtime_1 = os.path.getmtime(f'/s3-home-folder/{filename}')

        await stop_once()
        await delete_dir()

        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)

        await asyncio.sleep(1)

        start, stop_b = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop_b)
        await start()

        await asyncio.sleep(1)

        mtime_2 = os.path.getmtime(f'/s3-home-folder/{filename}')

        self.assertEqual(mtime_1, mtime_2)

    async def test_file_upload_preserves_mode_on_create(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop_a = syncer_for('/s3-home-folder')
        stopped = False

        async def stop_once():
            nonlocal stopped
            if stopped:
                return
            stopped = True
            await stop_a()
        self.addAsyncCleanup(stop_once)
        await start()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        os.chmod(f'/s3-home-folder/{filename}', 0o100600)

        await asyncio.sleep(1)

        await stop_once()
        await delete_dir()

        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)

        start, stop_b = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop_b)
        await start()

        await asyncio.sleep(1)

        mode = os.stat(f'/s3-home-folder/{filename}').st_mode

        self.assertEqual(mode, 0o100600)

    async def test_file_upload_preserves_mode_after_create(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop_a = syncer_for('/s3-home-folder')
        stopped = False

        async def stop_once():
            nonlocal stopped
            if stopped:
                return
            stopped = True
            await stop_a()
        self.addAsyncCleanup(stop_once)
        await start()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await asyncio.sleep(1)

        os.chmod(f'/s3-home-folder/{filename}', 0o100600)

        await stop_once()
        await delete_dir()

        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)

        await asyncio.sleep(1)

        start, stop_b = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop_b)
        await start()

        await asyncio.sleep(1)

        mode = os.stat(f'/s3-home-folder/{filename}').st_mode

        self.assertEqual(mode, 0o100600)

    async def test_file_download_then_upload_preserves_mode(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        filename = str(uuid.uuid4())
        response = await put_body(client, filename, b'some-bytes')
        self.assertEqual(response.status_code, 200)

        start, stop_a = syncer_for('/s3-home-folder')
        stopped = False

        async def stop_once():
            nonlocal stopped
            if stopped:
                return
            stopped = True
            await stop_a()
        self.addAsyncCleanup(stop_once)
        await start()

        # filename = str(uuid.uuid4())
        # with open(f'/s3-home-folder/{filename}', 'wb') as file:
        #     file.write(b'some-bytes')

        await asyncio.sleep(1)

        os.chmod(f'/s3-home-folder/{filename}', 0o100600)

        await stop_once()
        await delete_dir()

        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)

        await asyncio.sleep(1)

        start, stop_b = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop_b)
        await start()

        await asyncio.sleep(1)

        mode = os.stat(f'/s3-home-folder/{filename}').st_mode

        self.assertEqual(mode, 0o100600)

    async def test_single_small_file_uploaded_emoji(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
        await start()

        filename = str(uuid.uuid4()) + '_🍰'
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_body(client, filename), b'some-bytes')

    async def test_single_empty_file_uploaded(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
        await start()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb'):
            pass

        await await_upload()

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_body(client, filename), b'')

    async def test_file_inside_directory_after_delay(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
        await start()

        directory_name = str(uuid.uuid4())
        filename = str(uuid.uuid4())
        os.mkdir('/s3-home-folder/' + directory_name)

        await asyncio.sleep(0.1)

        with open(f'/s3-home-folder/{directory_name}/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_body(client, f'{directory_name}/{filename}'), b'some-bytes')

    async def test_file_inside_directory_immediate(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
        await start()

        directory_name = str(uuid.uuid4())
        filename = str(uuid.uuid4())
        os.mkdir('/s3-home-folder/' + directory_name)

        with open(f'/s3-home-folder/{directory_name}/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_body(client, f'{directory_name}/{filename}'), b'some-bytes')

    async def test_file_inside_nested_directory_immediate_after_previous_deleted(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
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

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_body(client, f'{directory_2}/{filename}'), b'some-bytes')

    async def test_nested_file_inside_directory_immediate(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
        await start()

        directory_1 = str(uuid.uuid4())
        directory_2 = str(uuid.uuid4())
        filename = str(uuid.uuid4())
        os.mkdir(f'/s3-home-folder/{directory_1}')
        os.mkdir(f'/s3-home-folder/{directory_1}/{directory_2}')

        with open(f'/s3-home-folder/{directory_1}/{directory_2}/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        body_bytes = await object_body(client, f'{directory_1}/{directory_2}/{filename}')
        self.assertEqual(body_bytes, b'some-bytes')

    async def test_file_uploaded_after_stop(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        await start()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'\x00' * 10000000)

        await stop()

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_body(client, filename), b'\x00' * 10000000)

    async def test_file_closed_half_way_through_with_no_modification(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
        await start()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'\x00' * 10000000)

        await asyncio.sleep(0)
        await asyncio.sleep(0)

        with open(f'/s3-home-folder/{filename}', 'a') as file:
            pass

        await await_upload()

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_body(client, filename), b'\x00' * 10000000)

    async def test_file_modified_and_closed_half_way_through(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
        await start()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'\x00' * 10000000)

        await asyncio.sleep(0)
        await asyncio.sleep(0)

        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'\x01' * 10000000)

        await await_upload()

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_body(client, filename), b'\x01' * 10000000)

    async def test_file_changed_half_way_through_no_close_then_close(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
        await start()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'\x00' * 10000000)

        await asyncio.sleep(0)
        await asyncio.sleep(0)

        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'\x01' * 10000000)

            await await_upload()

            client = get_docker_link_and_minio_compatible_http_pool()
            self.addAsyncCleanup(client.aclose)

            self.assertEqual(await object_code(client, filename), 404)

        await await_upload()
        self.assertEqual(await object_body(client, filename), b'\x01' * 10000000)

    async def test_single_small_file_deleted_after_delay(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
        await start()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()

        os.remove(f'/s3-home-folder/{filename}')

        await await_upload()

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_code(client, filename), 404)

    async def test_single_small_file_deleted_immediate(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
        await start()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        os.remove(f'/s3-home-folder/{filename}')

        await await_upload()

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_code(client, filename), 404)

    async def test_single_small_file_parent_directory_deleted_then_recreated_immediate(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
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

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_body(client, f'{dirname}/{filename}'), b'some-bytes')

    async def test_single_small_file_parent_directory_deleted_then_recreated_after_delay(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
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

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_body(client, f'{dirname}/{filename}'), b'some-bytes')

    async def test_file_in_renamed_directory_after_delay(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
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

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_code(client, f'{dirname_1}/{filename}'), 404)
        self.assertEqual(await object_body(client, f'{dirname_2}/{filename}'), b'some-bytes')

    async def test_file_in_renamed_directory_immediate(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
        await start()

        dirname_1 = str(uuid.uuid4())
        dirname_2 = str(uuid.uuid4())

        os.mkdir(f'/s3-home-folder/{dirname_1}')
        filename = str(uuid.uuid4())

        with open(f'/s3-home-folder/{dirname_1}/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        os.rename(f'/s3-home-folder/{dirname_1}', f'/s3-home-folder/{dirname_2}')

        await await_upload()

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_code(client, f'{dirname_1}/{filename}'), 404)
        self.assertEqual(await object_body(client, f'{dirname_2}/{filename}'), b'some-bytes')

    async def test_file_in_renamed_nested_directory_after_delay(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
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

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(
            await object_code(client, f'{dirname_1}/{dirname_2}/{filename}'),
            404)
        self.assertEqual(
            await object_body(client, f'{dirname_3}/{dirname_2}/{filename}'),
            b'some-bytes')

    async def test_file_created_in_renamed_watched_directory_after_delay(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
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

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_code(client, f'{dirname_1}/{filename}'), 404)
        self.assertEqual(await object_body(client, f'{dirname_2}/{filename}'), b'some-bytes')

    async def test_file_created_in_renamed_watched_directory_immediate(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
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

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_code(client, f'{dirname_1}/{filename}'), 404)
        self.assertEqual(await object_body(client, f'{dirname_2}/{filename}'), b'some-bytes')

    async def test_file_in_renamed_nested_directory_immediate(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
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

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(
            await object_code(client, f'{dirname_1}/{dirname_2}/{filename}'),
            404)
        self.assertEqual(
            await object_body(client, f'{dirname_3}/{dirname_2}/{filename}'),
            b'some-bytes')

    async def test_file_in_renamed_twice_nested_directory_after_delay(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
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

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(
            await object_code(client, f'{dirname_1}/{dirname_2}/{filename}'),
            404)
        self.assertEqual(
            await object_code(client, f'{dirname_3}/{dirname_2}/{filename}'),
            404)
        self.assertEqual(
            await object_body(client, f'{dirname_4}/{dirname_2}/{filename}'),
            b'some-bytes')

    async def test_file_in_renamed_twice_nested_directory_immediate(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
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

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(
            await object_code(client, f'{dirname_1}/{dirname_2}/{filename}'),
            404)
        self.assertEqual(
            await object_code(client, f'{dirname_3}/{dirname_2}/{filename}'),
            404)
        self.assertEqual(
            await object_body(client, f'{dirname_4}/{dirname_2}/{filename}'),
            b'some-bytes')

    async def test_file_rename_immediate(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
        await start()

        filename_1 = str(uuid.uuid4())
        filename_2 = str(uuid.uuid4())

        with open(f'/s3-home-folder/{filename_1}', 'wb') as file:
            file.write(b'some-bytes')

        os.rename(f'/s3-home-folder/{filename_1}', f'/s3-home-folder/{filename_2}')

        await await_upload()

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_code(client, f'{filename_1}'), 404)
        self.assertEqual(await object_body(client, f'{filename_2}'), b'some-bytes')

    async def test_file_rename_after_delay(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
        await start()

        filename_1 = str(uuid.uuid4())
        filename_2 = str(uuid.uuid4())

        with open(f'/s3-home-folder/{filename_1}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()

        os.rename(f'/s3-home-folder/{filename_1}', f'/s3-home-folder/{filename_2}')

        await await_upload()

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_code(client, f'{filename_1}'), 404)
        self.assertEqual(await object_body(client, f'{filename_2}'), b'some-bytes')

    async def test_file_delete_immediate(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
        await start()

        dirname = str(uuid.uuid4())
        os.mkdir(f'/s3-home-folder/{dirname}')
        filename = str(uuid.uuid4())

        with open(f'/s3-home-folder/{dirname}/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        os.remove(f'/s3-home-folder/{dirname}/{filename}')

        await await_upload()

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_code(client, f'{dirname}/{filename}'), 404)

    async def test_file_delete_after_delay(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
        await start()

        dirname = str(uuid.uuid4())
        os.mkdir(f'/s3-home-folder/{dirname}')
        filename = str(uuid.uuid4())

        with open(f'/s3-home-folder/{dirname}/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()

        os.remove(f'/s3-home-folder/{dirname}/{filename}')

        await await_upload()

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_code(client, f'{dirname}/{filename}'), 404)

    async def test_many_files_delete_after_delay(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
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

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        for filename in filenames:
            self.assertEqual(await object_code(client, f'{dirname}/{filename}'), 404)

    async def test_file_named_as_flush_uploaded_with_others(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        self.addAsyncCleanup(stop)
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

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)
        self.assertEqual(await object_body(client, f'{dirname}/{filename_1}'), b'some-bytes')
        self.assertEqual(await object_body(client, f'{dirname}/{filename_2}'), b'more-bytes')

    async def test_file_created_after_overflow(self):

        max_queued_events = 16384

        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        start, stop = syncer_for('/s3-home-folder')
        await start()

        await await_upload()

        filename_1 = str(uuid.uuid4())
        filename_2 = str(uuid.uuid4())

        for _ in range(0, max_queued_events):
            with open(f'/s3-home-folder/{filename_1}', 'wb') as file:
                file.write(b'some-bytes')
            os.remove(f'/s3-home-folder/{filename_1}')

        with open(f'/s3-home-folder/{filename_2}', 'wb') as file:
            file.write(b'more-bytes')

        await stop()

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_body(client, filename_2), b'more-bytes')

    async def test_multiple_syncers_on_same_folder(self):
        self.addAsyncCleanup(create_directory('/s3-home-folder'))
        self.addAsyncCleanup(create_directory('/test-data/my-bucket'))

        # We have to exclude the mobius flush files otherwise we end up in an infinite loop
        # where each syncer responds to the creation/deletion of the other's flush files
        start_1, stop_1 = syncer_for(
            '/s3-home-folder', exclude_local=r'.*(/|^)\.__mobius3_flush__.*')
        self.addAsyncCleanup(stop_1)
        await start_1()

        start_2, stop_2 = syncer_for(
            '/s3-home-folder', exclude_local=r'.*(/|^)\.__mobius3_flush__.*')
        self.addAsyncCleanup(stop_2)
        await start_2()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_body(client, f'{filename}'), b'some-bytes')

    async def test_multiple_syncers_on_nfs(self):
        # We could have mobius3 running on two different volumes, which are linked via some
        # mechanism unknown to inotify, such as NFS. The main relevant bebaviour of NFS is that
        # remote changes don't trigger inotify events, but the files/changes are there if looked
        # for. This is tricky to simulate in a test: we do the best we can by having a docker
        # volume mounted in two places, running mobius3 on each and ignoring any events on files
        # known to be created by the other

        exclude_local_1 = r'(.*(/|^)\.__mobius3_flush__.*)|(.*from_2.*)'
        exclude_local_2 = r'(.*(/|^)\.__mobius3_flush__.*)|(.*from_1.*)'

        #####

        # Meta test to make sure that the creation in one folder does not trigger an upload in the
        # other. We do this by having the linked folders sync to two separate buckets, and checking
        # that an upload to one does _not trigger an upload to the other

        self.addAsyncCleanup(create_directory('/nfs-1/s3-home-folder'))
        self.addAsyncCleanup(create_directory('/test-data/my-bucket-1'))

        start_1, stop_1 = syncer_for('/nfs-1/s3-home-folder',
                                     bucket='my-bucket-1',
                                     exclude_local=exclude_local_1,
                                     )
        self.addAsyncCleanup(stop_1)
        await start_1()

        self.addAsyncCleanup(create_directory('/test-data/my-bucket-2'))

        start_2, stop_2 = syncer_for('/nfs-2/s3-home-folder',
                                     bucket='my-bucket-2',
                                     exclude_local=exclude_local_2,
                                     )
        self.addAsyncCleanup(stop_2)
        await start_2()

        filename_1 = 'from_1_' + str(uuid.uuid4())
        with open(f'/nfs-1/s3-home-folder/{filename_1}', 'wb') as file:
            file.write(b'some-bs')

        # Check that the folders _are_ linked
        with open(f'/nfs-2/s3-home-folder/{filename_1}', 'rb') as file:
            contents = file.read()
        self.assertEqual(contents, b'some-bs')

        await await_upload()

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_body(client, filename_1, bucket='my-bucket-1'), b'some-bs')
        self.assertEqual(await object_code(client, filename_1, bucket='my-bucket-2'), 404)

        #####

        # Ensure that even after polling S3, there is no change of mtime, and something horrible
        # like the file disappearing hasn't happened

        self.addAsyncCleanup(create_directory('/test-data/my-bucket'))

        start_1, stop_1 = syncer_for('/nfs-1/s3-home-folder',
                                     exclude_local=exclude_local_1,
                                     local_modification_persistance=1, download_interval=1,
                                     )
        self.addAsyncCleanup(stop_1)
        await start_1()

        start_2, stop_2 = syncer_for('/nfs-2/s3-home-folder',
                                     exclude_local=exclude_local_2,
                                     local_modification_persistance=1, download_interval=1,
                                     )
        self.addAsyncCleanup(stop_2)
        await start_2()

        filename_1 = 'from_1_' + str(uuid.uuid4())
        with open(f'/nfs-1/s3-home-folder/{filename_1}', 'wb') as file:
            file.write(b'some-bytes')
        mtime = os.path.getmtime(f'/nfs-1/s3-home-folder/{filename_1}')

        await await_upload()

        self.assertEqual(await object_body(client, filename_1), b'some-bytes')

        await asyncio.sleep(2)

        self.assertEqual(mtime, os.path.getmtime(f'/nfs-1/s3-home-folder/{filename_1}'))
        self.assertEqual(mtime, os.path.getmtime(f'/nfs-2/s3-home-folder/{filename_1}'))

        #####

        # Ensure that deleting a file works as expected

        os.unlink(f'/nfs-1/s3-home-folder/{filename_1}')

        await await_upload()

        self.assertEqual(await object_code(client, filename_1), 404)

        await asyncio.sleep(2)

        self.assertFalse(os.path.exists(f'/nfs-1/s3-home-folder/{filename_1}'))
        self.assertFalse(os.path.exists(f'/nfs-2/s3-home-folder/{filename_1}'))


class TestEndToEnd(unittest.IsolatedAsyncioTestCase):

    async def test_console_script(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        mobius3_process = await asyncio.create_subprocess_exec(
            'mobius3', '/s3-home-folder', 'my-bucket', 'https://minio:9000/{}/', 'us-east-1',
            '--cloudwatch-monitoring-endpoint', 'http://motoserver:3000/',
            '--cloudwatch-monitoring-region', 'us-east-1',
            '--cloudwatch-monitoring-namespace', 'MyApp/Testing',
            '--disable-ssl-verification',
            env=os.environ, stdout=asyncio.subprocess.PIPE,
        )
        self.addAsyncCleanup(terminate, mobius3_process)

        await await_upload()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()
        await await_upload()

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_body(client, filename), b'some-bytes')

        await await_upload()

    async def test_direct_script_ecs_auth(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        await set_temporary_creds(client)

        mobius3_process = await asyncio.create_subprocess_exec(
            sys.executable, '-m', 'mobius3',
            '/s3-home-folder', 'my-bucket', 'https://minio:9000/{}/', 'us-east-1',
            '--disable-ssl-verification',
            '--credentials-source', 'ecs-container-endpoint',
            env=os.environ, stdout=asyncio.subprocess.PIPE,
        )
        self.addAsyncCleanup(terminate, mobius3_process)

        await await_upload()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()
        await await_upload()

        self.assertEqual(await object_body(client, filename), b'some-bytes')

        await await_upload()

    async def test_direct_script_no_upload_existing(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'some-original-bytes')

        mobius3_process = await asyncio.create_subprocess_exec(
            sys.executable, '-m', 'mobius3',
            '/s3-home-folder', 'my-bucket', 'https://minio:9000/{}/', 'us-east-1',
            '--disable-ssl-verification',
            env=os.environ, stdout=asyncio.subprocess.PIPE,
        )
        self.addAsyncCleanup(terminate, mobius3_process)

        await await_upload()
        await await_upload()

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_code(client, filename), 404)

    async def test_direct_script_delay(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        mobius3_process = await asyncio.create_subprocess_exec(
            sys.executable, '-m', 'mobius3',
            '/s3-home-folder', 'my-bucket', 'https://minio:9000/{}/', 'us-east-1',
            '--disable-ssl-verification',
            env=os.environ, stdout=asyncio.subprocess.PIPE,
        )
        self.addAsyncCleanup(terminate, mobius3_process)

        await await_upload()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        await await_upload()
        await await_upload()

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_body(client, filename), b'some-bytes')

        await await_upload()

    async def test_direct_script_after_stop(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        mobius3_process = await asyncio.create_subprocess_exec(
            sys.executable, '-m', 'mobius3',
            '/s3-home-folder', 'my-bucket', 'https://minio:9000/{}/', 'us-east-1',
            '--disable-ssl-verification',
            env=os.environ, stdout=asyncio.subprocess.PIPE,
        )
        self.addAsyncCleanup(terminate, mobius3_process)

        await await_upload()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        mobius3_process.terminate()
        await mobius3_process.wait()

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_body(client, filename), b'some-bytes')

        await await_upload()

    async def test_direct_script_without_prefix_after_stop(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        mobius3_process = await asyncio.create_subprocess_exec(
            sys.executable, '-m', 'mobius3',
            '/s3-home-folder', 'my-bucket', 'https://minio:9000/{}/', 'us-east-1',
            '--disable-ssl-verification',
            env=os.environ, stdout=asyncio.subprocess.PIPE,
        )
        self.addAsyncCleanup(terminate, mobius3_process)

        await await_upload()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        mobius3_process.terminate()
        await mobius3_process.wait()

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_body(client, filename), b'some-bytes')

        await await_upload()

    async def test_direct_script_with_prefix_after_stop(self):
        delete_dir = create_directory('/s3-home-folder')
        self.addAsyncCleanup(delete_dir)
        delete_bucket_dir = create_directory('/test-data/my-bucket')
        self.addAsyncCleanup(delete_bucket_dir)

        mobius3_process = await asyncio.create_subprocess_exec(
            sys.executable, '-m', 'mobius3',
            '/s3-home-folder', 'my-bucket', 'https://minio:9000/{}/', 'us-east-1',
            '--prefix', 'my-folder/',
            '--disable-ssl-verification',
            env=os.environ, stdout=asyncio.subprocess.PIPE,
        )
        self.addAsyncCleanup(terminate, mobius3_process)

        await await_upload()

        filename = str(uuid.uuid4())
        with open(f'/s3-home-folder/{filename}', 'wb') as file:
            file.write(b'some-bytes')

        mobius3_process.terminate()
        await mobius3_process.wait()

        client = get_docker_link_and_minio_compatible_http_pool()
        self.addAsyncCleanup(client.aclose)

        self.assertEqual(await object_body(client, f'my-folder/{filename}'), b'some-bytes')

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
    ssl_context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_CLIENT)
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    return Pool(
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


def syncer_for(path, bucket='my-bucket', prefix='',
               local_modification_persistance=120, download_interval=60,
               exclude_remote='^$',
               exclude_local='^$',
               upload_on_create='^$',):
    return Syncer(
        path, bucket, 'https://minio:9000/{}/', 'us-east-1',
        get_pool=get_docker_link_and_minio_compatible_http_pool,
        prefix=prefix,
        local_modification_persistance=local_modification_persistance,
        download_interval=download_interval,
        exclude_remote=exclude_remote,
        exclude_local=exclude_local,
        upload_on_create=upload_on_create,
        cloudwatch_monitoring_endpoint='http://motoserver:3000/',
        cloudwatch_monitoring_region='us-east-1',
        cloudwatch_monitoring_namespace='MyApp/Testing',
    )


async def await_upload():
    await asyncio.sleep(1)


async def object_body(client, key, bucket='my-bucket'):
    return (await object_response(client, key, bucket=bucket)).content


async def object_code(client, key, bucket='my-bucket'):
    return (await object_response(client, key, bucket=bucket)).status_code


async def get_credentials_from_environment(_):
    return os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'], ()


async def object_response(client, key, bucket='my-bucket'):
    return await client.request('GET', f'https://minio:9000/{bucket}/{key}', auth=AWSAuth(
        service='s3', region='us-east-1', client=client, get_credentials=get_credentials_from_environment
    ))


async def delete_object(client, key):
    return await client.request('DELETE', f'https://minio:9000/my-bucket/{key}', auth=AWSAuth(
        service='s3', region='us-east-1', client=client, get_credentials=get_credentials_from_environment
    ))


async def put_body(client, key, body):
    content_hash, hashed_content = get_content_hash(body)
    auth = AWSAuth(
        service='s3', region='us-east-1', client=client, get_credentials=get_credentials_from_environment,
        content_hash=content_hash
    )
    return await client.request('PUT', f'https://minio:9000/my-bucket/{key}', headers=(
        ('content-length', str(len(body))),
    ), content=hashed_content, auth=auth)


async def set_temporary_creds(client):
    admin_access_key_id, admin_secret_access_key, _ = await get_credentials_from_environment(client)

    # minio doesn't seem to be able to give temporary creds for the main user
    user_access_key_id = str(uuid.uuid4())[:8]
    user_secret_access_key = str(uuid.uuid4())[:8]

    async def new_user_creds(_):
        return user_access_key_id, user_secret_access_key, ()

    proc = await asyncio.create_subprocess_exec(
        './mc', '--insecure', 'config', 'host', 'add', 'myminio',
        'https://minio:9000', admin_access_key_id, admin_secret_access_key,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)
    stdout, stderr = await proc.communicate()
    if proc.returncode:
        raise Exception(stdout + stderr)

    proc = await asyncio.create_subprocess_exec(
        './mc', '--insecure', 'admin', 'user', 'add', 'myminio',
        user_access_key_id, user_secret_access_key,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)
    stdout, stderr = await proc.communicate()
    if proc.returncode:
        raise Exception(stdout + stderr)

    proc = await asyncio.create_subprocess_exec(
        './mc', '--insecure', 'admin', 'policy', 'set', 'myminio', 'readwrite',
        'user=' + user_access_key_id,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)
    stdout, stderr = await proc.communicate()
    if proc.returncode:
        raise Exception(stdout + stderr)

    request_body_bytes = urllib.parse.urlencode((
        ('Action', 'AssumeRole'),
        ('Version', '2011-06-15'),
    )).encode('utf-8')

    content_hash, hashed_content = get_content_hash(request_body_bytes)
    auth = AWSAuth(service='sts', region='us-east-1', client=client, get_credentials=new_user_creds, content_hash=content_hash)
    response = await client.request(
        'POST', 'https://minio:9000/',
        headers=(
            ('content-type', 'application/x-www-form-urlencoded; charset=utf-8'),
            ('content-length', str(len(request_body_bytes))),
        ),
        content=hashed_content,
        auth=auth
    )

    def xml(tag):
        return re.search(b'<' + tag + b'>(.*)</' + tag + b'>', response.content)[1].decode('utf-8')

    creds = {
        'AccessKeyId': xml(b'AccessKeyId'),
        'SecretAccessKey': xml(b'SecretAccessKey'),
        'Expiration': xml(b'Expiration'),
        'Token': xml(b'SessionToken'),
    }

    request_content_bytes = json.dumps(creds).encode('utf-8')
    await client.request(
        'POST', 'http://169.254.170.2/creds', content=streamed(request_content_bytes), headers=(
            ('content-length', str(len(request_content_bytes))),
        ))
    return creds


def get_content_hash(content=b''):

    async def hashed_content():
        yield content

    return hashlib.sha256(content).hexdigest(), hashed_content()
