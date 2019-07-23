import asyncio
import os
import shutil
import ssl
import unittest
import uuid

from aiodnsresolver import (
    Resolver,
)
from lowhaio import (
    Pool,
    buffered,
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
    async def test_single_small_file_uploaded(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)

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
    async def test_single_empty_file_uploaded(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)

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
    async def test_nested_file_inside_directory_immediate(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)

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
    async def test_file_closed_half_way_through_with_no_modification(self):
        delete_dir = create_directory('/s3-home-folder')
        self.add_async_cleanup(delete_dir)

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


def create_directory(path):
    async def delete_dir():
        shutil.rmtree(path)

    os.mkdir(path)

    return delete_dir


def get_docker_link_and_minio_compatible_http_pool():
    async def transform_fqdn(fqdn):
        return fqdn

    ssl_context = ssl.SSLContext()
    ssl_context.verify_mode = ssl.CERT_NONE

    return Pool(
        # 0x20 encoding does not appear to work with linked containers
        get_dns_resolver=lambda: Resolver(transform_fqdn=transform_fqdn),
        # We use self-signed certs locally
        get_ssl_context=lambda: ssl_context,
    )


def syncer_for(path):
    return Syncer(
        path, 'https://minio:9000/my-bucket', 'us-east-1',
        get_pool=get_docker_link_and_minio_compatible_http_pool,
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


async def object_triple(request, key):
    async def get_credentials_from_environment():
        return os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'], ()

    signed_request = signed(
        request, credentials=get_credentials_from_environment,
        service='s3', region='us-east-1',
    )
    return await signed_request(b'GET', f'https://minio:9000/my-bucket/{key}')
