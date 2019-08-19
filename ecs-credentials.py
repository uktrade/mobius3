'''Mock ecs-credentials server

We don't use the AWS-provided mock for two reasons:

- Unable to get it to send an expiration date, seemed to always be blank,
  while Fargate always seems to set them.

- It doesn't seem controllable. Minio can't be told a session token, it
  must generate one. So the tests have to get a session token from Minio
  in order to tell this server to provide it to the application to send to
  Minio
'''
import asyncio
import os

from aiohttp import (
    web,
)


async def async_main(container_credentials_relative_uri):

    creds = b''

    async def set_creds(request):
        nonlocal creds
        creds = await request.read()
        return web.Response(body=b'')

    async def provide_creds(_):
        return web.Response(body=creds)

    app = web.Application()
    app.add_routes([
        web.get(container_credentials_relative_uri, provide_creds),
        web.post(container_credentials_relative_uri, set_creds),
    ])

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 80)
    await site.start()


def main():
    container_credentials_relative_uri = os.environ['AWS_CONTAINER_CREDENTIALS_RELATIVE_URI']

    loop = asyncio.get_event_loop()
    loop.create_task(async_main(container_credentials_relative_uri))
    loop.run_forever()


if __name__ == '__main__':
    main()
