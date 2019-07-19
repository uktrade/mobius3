import asyncio
import logging
import os
import ssl
import sys
import unittest

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

    @async_test
    async def test_single_small_file_uploaded(self):
        self.assertTrue(Syncer)

        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        logger.addHandler(handler)

        # Connection pool that is docker-link-friendly

        async def transform_fqdn(fqdn):
            return fqdn
        ssl_context = ssl.SSLContext()
        ssl_context.verify_mode = ssl.CERT_NONE

        def get_pool():
            return Pool(
                # 0x20 encoding does not appear to work with linked containers
                get_dns_resolver=lambda: Resolver(transform_fqdn=transform_fqdn),
                # We use self-signed certs locally
                get_ssl_context=lambda: ssl_context,
            )

        # Start syncing
        os.mkdir('/s3-home-folder')
        start, _ = Syncer(
            '/s3-home-folder', 'https://minio:9000/my-bucket', 'us-east-1',
            get_pool=get_pool,
        )
        await start()

        # Create test file to be uploaded
        with open('/s3-home-folder/file', 'wb') as file:
            file.write(b'some-bytes')

        # Wait for upload
        await asyncio.sleep(1)

        # Check if file uploaded to bucket
        request, _ = get_pool()

        async def get_credentials_from_environment():
            return os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'], ()

        signed_request = signed(
            request, credentials=get_credentials_from_environment,
            service='s3', region='us-east-1',
        )
        _, _, body = await signed_request(b'GET', 'https://minio:9000/my-bucket/file')
        body_bytes = await buffered(body)
        self.assertEqual(body_bytes, b'some-bytes')
