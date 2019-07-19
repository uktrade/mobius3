import asyncio
import unittest

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
    async def test_dummy(self):
        self.assertTrue(Syncer)
