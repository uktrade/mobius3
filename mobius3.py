import array
import argparse
import asyncio
import ctypes
import datetime
import enum
import fcntl
import termios
import json
import logging
import os
import signal
import ssl
import sys
import uuid
import urllib.parse
from pathlib import (
    PurePosixPath,
)
import struct
from weakref import (
    WeakValueDictionary,
)
from xml.etree import (
    ElementTree as ET,
)

from aiodnsresolver import (
    ResolverLoggerAdapter,
    Resolver,
)
from fifolock import (
    FifoLock,
)
from lowhaio import (
    HttpLoggerAdapter,
    Pool,
    buffered,
    empty_async_iterator,
    timeout,
)
from lowhaio_aws_sigv4_unsigned_payload import (
    aws_sigv4_headers,
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


class InotifyEvents(enum.IntEnum):
    IN_MODIFY = 0x00000002
    IN_CLOSE_WRITE = 0x00000008
    IN_MOVED_FROM = 0x00000040
    IN_MOVED_TO = 0x00000080
    IN_CREATE = 0x00000100
    IN_DELETE = 0x00000200

    # Sent by the kernel without explicitly watching for them
    IN_Q_OVERFLOW = 0x00004000
    IN_IGNORED = 0x00008000


class InotifyFlags(enum.IntEnum):
    IN_ONLYDIR = 0x01000000
    IN_ISDIR = 0x40000000


class S3SyncLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return \
            ('[s3sync] %s' % (msg,), kwargs) if not self.extra else \
            ('[s3sync:%s] %s' % (','.join(str(v) for v in self.extra.values()), msg), kwargs)


def child_adapter(s3sync_adapter, extra):
    return S3SyncLoggerAdapter(
        s3sync_adapter.logger,
        {**s3sync_adapter.extra, **extra},
    )


def get_logger_adapter_default(extra):
    return S3SyncLoggerAdapter(logging.getLogger('mobius3'), extra)


def get_http_logger_adapter_default(s3sync_extra):
    def _get_http_logger_adapter_default(http_extra):
        s3sync_adapter = S3SyncLoggerAdapter(logging.getLogger('lowhaio'), s3sync_extra)
        return HttpLoggerAdapter(s3sync_adapter, http_extra)
    return _get_http_logger_adapter_default


def get_resolver_logger_adapter_default(s3sync_extra):
    def _get_resolver_logger_adapter_default(http_extra):
        def __get_resolver_logger_adapter_default(resolver_extra):
            s3sync_adapter = S3SyncLoggerAdapter(logging.getLogger('aiodnsresolver'), s3sync_extra)
            http_adapter = HttpLoggerAdapter(s3sync_adapter, http_extra)
            return ResolverLoggerAdapter(http_adapter, resolver_extra)
        return __get_resolver_logger_adapter_default
    return _get_resolver_logger_adapter_default


WATCH_MASK = \
    InotifyEvents.IN_MODIFY | \
    InotifyEvents.IN_CLOSE_WRITE | \
    InotifyEvents.IN_MOVED_FROM | \
    InotifyEvents.IN_MOVED_TO | \
    InotifyEvents.IN_CREATE | \
    InotifyEvents.IN_DELETE | \
    InotifyFlags.IN_ONLYDIR


EVENT_HEADER = struct.Struct('iIII')


async def get_credentials_from_environment(_):
    return os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'], ()


def get_credentials_from_ecs_endpoint():
    aws_access_key_id = None
    aws_secret_access_key = None
    pre_auth_headers = None
    expiration = datetime.datetime.fromtimestamp(0)

    async def _get_credentials(request):
        nonlocal aws_access_key_id
        nonlocal aws_secret_access_key
        nonlocal pre_auth_headers
        nonlocal expiration

        now = datetime.datetime.now()

        if now > expiration:
            _, _, body = await request(
                b'GET',
                'http://169.254.170.2' + os.environ['AWS_CONTAINER_CREDENTIALS_RELATIVE_URI']
            )
            creds = json.loads(await buffered(body))
            aws_access_key_id = creds['AccessKeyId']
            aws_secret_access_key = creds['SecretAccessKey']
            pre_auth_headers = (
                (b'x-amz-security-token', creds['Token'].encode(),),
            )

        return aws_access_key_id, aws_secret_access_key, pre_auth_headers

    return _get_credentials


def Syncer(
        directory, bucket, region,
        prefix='',
        concurrent_uploads=5,
        concurrent_downloads=5,
        get_credentials=get_credentials_from_environment,
        get_pool=Pool,
        flush_file_root='.__mobius3__',
        flush_file_timeout=5,
        get_logger_adapter=get_logger_adapter_default,
        get_http_logger_adapter=get_http_logger_adapter_default,
        get_resolver_logger_adapter=get_resolver_logger_adapter_default,
):

    loop = asyncio.get_running_loop()
    logger = get_logger_adapter({})

    directory = PurePosixPath(directory)

    # The file descriptor returned from inotify_init
    fd = None

    # Watch descriptors to paths. A notification returns only a relative
    # path to its watch descriptor path: these are used to find the full
    # path of any notified-on files
    wds_to_path = {}

    # The asyncio task pool that performs the uploads
    upload_tasks = []

    # PUTs and DELETEs are initiated in the order generated by inotify events
    upload_job_queue = asyncio.Queue()

    download_tasks = []
    download_job_queue = asyncio.Queue()

    # To prevent concurrent HTTP requests on the same files where order of
    # receipt by S3 cannot be guaranteed, we wrap each request by a lock
    path_locks = WeakValueDictionary()

    # A path -> content version dict is maintained during queues and uploads,
    # and incremented on every modification of a file. When a path is
    # scheduled to be uploaded, its version is copied. After the last read of
    # data for an upload, but before it's uploaded, the copied version of the
    # path is compared to the current version. If this is different, there was
    # a change to the file contents, we know another upload will be scheduled,
    # so we abort the current upload
    content_versions = WeakValueDictionary()

    # Before completing an upload, we force a flush of the event queue for
    # the uploads directory to ensure that we have processed any change events
    # that would upate the corresponding item in content_versions
    flushes = WeakValueDictionary()

    # A cache of the file tree is maintained. Used for directory renames: we
    # only get notified of renames _after_ they have happened, we need a way
    # to know what objects are on S3 in order to DELETE them
    tree_cache_root = {
        'type': 'directory',
        'children': {},
    }

    request, close_pool = get_pool()

    def signed(request, credentials, service, region):
        async def _signed(logger, method, url, params=(), headers=(),
                          body=empty_async_iterator, body_args=(), body_kwargs=(),
                          get_logger_adapter=get_http_logger_adapter,
                          get_resolver_logger_adapter=get_resolver_logger_adapter):

            body_hash = 'UNSIGNED-PAYLOAD'
            access_key_id, secret_access_key, auth_headers = await credentials(request)

            parsed_url = urllib.parse.urlsplit(url)
            all_headers = aws_sigv4_headers(
                access_key_id, secret_access_key, headers + auth_headers, service, region,
                parsed_url.hostname, method.decode(), parsed_url.path, params, body_hash,
            )

            return await request(
                method, url, params=params, headers=all_headers,
                body=body, body_args=body_args, body_kwargs=body_kwargs,
                get_logger_adapter=get_logger_adapter(logger.extra),
                get_resolver_logger_adapter=get_resolver_logger_adapter(logger.extra),
            )

        return _signed

    signed_request = signed(
        request, credentials=get_credentials, service='s3', region=region,
    )

    def ensure_file_in_tree_cache(path):
        directory = tree_cache_root
        for parent in reversed(list(path.parents)):
            directory = directory['children'].setdefault(parent.name, {
                'type': 'directory',
                'children': {},
            })
        directory['children'][path.name] = {
            'type': 'file',
        }

    def remove_file_from_tree_cache(path):
        directory = tree_cache_root
        for parent in reversed(list(path.parents)):
            directory = directory['children'][parent.name]
        del directory['children'][path.name]

    def tree_cache_directory(path):
        directory = tree_cache_root
        for parent in reversed(list(path.parents)):
            directory = directory['children'][parent.name]
        return directory['children'][path.name]

    async def start():
        logger = get_logger_adapter({'mobius3_component': 'start'})
        logger.info('Starting')
        nonlocal upload_tasks
        nonlocal download_tasks
        upload_tasks = [
            asyncio.create_task(process_jobs(upload_job_queue))
            for i in range(0, concurrent_uploads)
        ]
        download_tasks = [
            asyncio.create_task(process_jobs(download_job_queue))
            for i in range(0, concurrent_downloads)
        ]
        await list_and_schedule_downloads(logger)
        await download_job_queue.join()
        start_inotify(logger)
        logger.info('Finished starting')

    def start_inotify(logger):
        nonlocal wds_to_path
        nonlocal tree_cache_root
        nonlocal fd
        wds_to_path = {}
        tree_cache_root = {
            'type': 'directory',
            'children': {},
        }
        fd = call_libc(libc.inotify_init)

        def _read_events():
            logger = get_logger_adapter({'mobius3_component': 'event'})
            read_events(logger)

        loop.add_reader(fd, _read_events)
        watch_and_upload_directory(logger, directory)

    async def stop():
        # Make every effort to read all incoming events and finish the queue
        logger = get_logger_adapter({'mobius3_component': 'stop'})
        logger.info('Stopping')
        read_events(logger)
        while upload_job_queue._unfinished_tasks:
            await upload_job_queue.join()
            read_events(logger)
        stop_inotify()
        for task in upload_tasks:
            task.cancel()
        for task in download_tasks:
            task.cancel()
        await close_pool()
        await asyncio.sleep(0)
        logger.info('Finished stopping')

    def stop_inotify():
        loop.remove_reader(fd)
        os.close(fd)

    def watch_and_upload_directory(logger, path):
        try:
            wd = call_libc(libc.inotify_add_watch, fd, str(path).encode('utf-8'), WATCH_MASK)
        except (NotADirectoryError, FileNotFoundError):
            return

        # After a directory rename, we will be changing the path of an
        # existing entry, but that's fine
        wds_to_path[wd] = path

        # By the time we've added a watcher, files or subdirectories may have
        # already been created
        for root, dirs, files in os.walk(path):
            for file in files:
                logger.info('Scheduling upload: %s', PurePosixPath(root) / file)
                schedule_upload(logger, PurePosixPath(root) / file)

            for directory in dirs:
                watch_and_upload_directory(logger, PurePosixPath(root) / directory)

    def remote_delete_directory(logger, path):
        # Directory nesting not likely to be large
        def recursive_delete(prefix, directory):
            for child_name, child in list(directory['children'].items()):
                if child['type'] == 'file':
                    logger.info('Scheduling delete: %s', prefix / child_name)
                    schedule_delete(logger, prefix / child_name)
                else:
                    recursive_delete(prefix / child_name, child)

        try:
            cache_directory = tree_cache_directory(path)
        except KeyError:
            # We may be moving from or deleting something not yet watched,
            # in which case we leave S3 as it is. There may be file(s) in
            # the queue to upload, but they will correctly fail if it can't
            # find the file(s)
            pass
        else:
            recursive_delete(path, cache_directory)

    def read_events(parent_logger):
        FIONREAD_output = array.array('i', [0])
        fcntl.ioctl(fd, termios.FIONREAD, FIONREAD_output)
        bytes_to_read = FIONREAD_output[0]

        if not bytes_to_read:
            return
        raw_bytes = os.read(fd, bytes_to_read)

        offset = 0
        while offset < len(raw_bytes):
            wd, mask, _, length = EVENT_HEADER.unpack_from(raw_bytes, offset)
            offset += EVENT_HEADER.size
            path = PurePosixPath(raw_bytes[offset:offset+length].rstrip(b'\0').decode('utf-8'))
            offset += length

            event_id = uuid.uuid4().hex[:8]
            logger = child_adapter(parent_logger, {'event': event_id})

            if mask & InotifyEvents.IN_Q_OVERFLOW:
                logger.warning('IN_Q_OVERFLOW. Restarting')
                stop_inotify()
                start_inotify(logger)
                continue

            full_path = wds_to_path[wd] / path
            logger.debug('Path: %s', full_path)

            if path.name.startswith(flush_file_root):
                try:
                    flush = flushes[full_path]
                except KeyError:
                    logger.debug('Flush file not found')
                else:
                    logger.debug('Flushing')
                    flush.set()
                    continue

            events = [event for event in InotifyEvents.__members__.values() if event & mask]
            item_type = 'dir' if mask & InotifyFlags.IN_ISDIR else 'file'
            for event in events:
                handler_name = f'handle__{item_type}__{event.name}'
                logger.debug('Handler: %s', handler_name)
                try:
                    handler = parent_locals[handler_name]
                except KeyError:
                    logger.debug('Handler not found')
                    continue

                try:
                    handler(logger, wd, full_path)
                except Exception:
                    logger.exception('Exception calling handler')

    def handle__file__IN_CLOSE_WRITE(logger, _, path):
        schedule_upload(logger, path)

    def handle__dir__IN_CREATE(logger, _, path):
        watch_and_upload_directory(logger, path)

    def handle__file__IN_DELETE(logger, _, path):
        # Correctness does not depend on this bump: it's an optimisation
        # that ensures we abandon any upload of this path ahead of us
        # in the queue
        bump_content_version(path)
        schedule_delete(logger, path)

    def handle__file__IN_IGNORED(_, wd, __):
        # For some reason IN_ISDIR is not set with IN_IGNORED
        del wds_to_path[wd]

    def handle__file__IN_MODIFY(_, __, path):
        bump_content_version(path)

    def handle__dir__IN_MOVED_FROM(logger, _, path):
        remote_delete_directory(logger, path)

    def handle__file__IN_MOVED_FROM(logger, _, path):
        schedule_delete(logger, path)

    def handle__dir__IN_MOVED_TO(logger, _, path):
        watch_and_upload_directory(logger, path)

    def handle__file__IN_MOVED_TO(logger, _, path):
        schedule_upload(logger, path)

    def get_content_version(path):
        return content_versions.setdefault(path, default=WeakReferenceableDict(version=0))

    def bump_content_version(path):
        get_content_version(path)['version'] += 1

    def get_lock(path):
        return path_locks.setdefault(path, default=FifoLock())

    def schedule_upload(logger, path):
        version_current = get_content_version(path)
        version_original = version_current.copy()

        async def function():
            await upload(logger, path, version_current, version_original)

        ensure_file_in_tree_cache(path)
        upload_job_queue.put_nowait((logger, function))

    def schedule_delete(logger, path):
        async def function():
            await delete(logger, path)

        try:
            remove_file_from_tree_cache(path)
        except KeyError:
            # Create events for files do not register a file in the cache,
            # until they are scheduled for upload on modification. If this
            # doesn't happen, then the file won't be in the cache, and we
            # have nothing to upload
            return
        upload_job_queue.put_nowait((logger, function))

    async def process_jobs(queue):
        while True:
            logger, job = await queue.get()
            try:
                await job()
            except Exception as exception:
                if isinstance(exception, asyncio.CancelledError):
                    raise
                if (
                        isinstance(exception, FileContentChanged) or
                        isinstance(exception.__cause__, FileContentChanged)
                ):
                    logger.info('Content changed, aborting: %s', FileContentChanged)
                if (
                        isinstance(exception, FileNotFoundError) or
                        isinstance(exception.__cause__, FileNotFoundError)
                ):
                    logger.info('File not found: %s', FileNotFoundError)
                if (
                        not isinstance(exception, FileNotFoundError) and
                        not isinstance(exception.__cause__, FileNotFoundError) and
                        not isinstance(exception, FileContentChanged) and
                        not isinstance(exception.__cause__, FileContentChanged)
                ):
                    logger.exception('Exception during %s', job)
            finally:
                queue.task_done()

    async def upload(logger, path, content_version_current, content_version_original):
        logger.info('Uploading %s', path)

        async def flush_events():
            flush_path = path.parent / (flush_file_root + uuid.uuid4().hex)
            logger.debug('Creating flush file: %s', flush_path)
            event = asyncio.Event()
            flushes[flush_path] = event
            with open(flush_path, 'w'):
                pass
            os.remove(flush_path)
            # In rare cases, the event queue could be full and the event for
            # the flush file is dropped
            with timeout(loop, flush_file_timeout):
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
                        raise FileContentChanged(path)

                    yield chunk

        content_length = str(os.stat(path).st_size).encode()

        # Ensure we only progress if the content length hasn't changed since
        # we have queued the upload
        await flush_events()
        if content_version_current != content_version_original:
            raise FileContentChanged(path)

        await locked_request(logger, b'PUT', path, body=file_body,
                             headers=((b'content-length', content_length),))

    async def delete(logger, path):
        logger.info('Deleting %s', path)
        await locked_request(logger, b'DELETE', path)

    async def locked_request(logger, method, path, headers=(), body=empty_async_iterator):
        remote_url = bucket + prefix + str(path.relative_to(directory))

        async with get_lock(path)(Mutex):
            logger.debug('%s %s %s', method.decode(), remote_url, headers)
            code, headers, body = await signed_request(
                logger, method, remote_url, headers=headers, body=body)
            logger.debug('%s %s', code, headers)
            body_bytes = await buffered(body)

        if code not in [b'200', b'204']:
            raise Exception(code, body_bytes)

    async def list_and_schedule_downloads(logger):
        async for path in list_keys_relative_to_prefix(logger):
            logger.info('Scheduling download: %s', path)
            schedule_download(logger, path)

    def schedule_download(logger, path):
        async def download():
            logger.info('Downloading: %s', path)
            code, _, body = await signed_request(logger, b'GET', bucket + prefix + path)
            if code != b'200':
                raise Exception(code)

            parent_directory = directory / (PurePosixPath(path).parent)
            try:
                os.makedirs(parent_directory)
            except FileExistsError:
                logger.debug('Already exists: %s', parent_directory)
            except NotADirectoryError:
                logger.debug('Not a directory: %s', parent_directory)
            except Exception:
                logger.debug('Unable to create directory: %s', parent_directory)

            full_path = directory / path
            with open(full_path, 'wb') as file:
                async for chunk in body:
                    file.write(chunk)

        download_job_queue.put_nowait((logger, download))

    async def list_keys_relative_to_prefix(logger):
        async def _list(extra_query_items=()):
            query = (
                ('max-keys', '1000'),
                ('list-type', '2'),
                ('prefix', prefix),
            ) + extra_query_items
            code, _, body = await signed_request(logger, b'GET', bucket, params=query)
            body_bytes = await buffered(body)
            if code != b'200':
                raise Exception(code, body_bytes)

            namespace = '{http://s3.amazonaws.com/doc/2006-03-01/}'
            root = ET.fromstring(body_bytes)
            next_token = ''
            keys_relative = []
            for element in root:
                if element.tag == f'{namespace}Contents':
                    key = first_child_text(element, f'{namespace}Key')
                    key_relative = key[len(prefix):]
                    keys_relative.append(key_relative)

                if element.tag == f'{namespace}NextContinuationToken':
                    next_token = element.text

            return (next_token, keys_relative)

        async def list_first_page():
            return await _list()

        async def list_later_page(token):
            return await _list((('continuation-token', token),))

        def first_child_text(element, tag):
            for child in element:
                if child.tag == tag:
                    return child.text
            return None

        token, keys_page = await list_first_page()
        for key in keys_page:
            yield key

        while token:
            token, keys_page = await list_later_page(token)
            for key in keys_page:
                yield key

    parent_locals = locals()

    return start, stop


async def async_main(syncer_args):
    start, stop = Syncer(**syncer_args)
    await start()
    return stop


def main():
    parser = argparse.ArgumentParser(prog='mobius3', formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument(
        'directory',
        metavar='directory',
        help='Path of the directory to sync, without a trailing slash\ne.g. /path/to/dir')
    parser.add_argument(
        'bucket',
        metavar='bucket',
        help='URL to the remote bucket, with a trailing slash\n'
             'e.g. https://s3-eu-west-2.amazonaws.com/my-bucket-name/')
    parser.add_argument(
        'region',
        metavar='region',
        help='The region of the bucket\ne.g. eu-west-2')

    parser.add_argument(
        '--credentials-source',
        metavar='credentials-source',
        default='envrionment-variables',
        nargs='?',
        choices=['environment-variables', 'ecs-container-endpoint'],
        help='Where to pickup AWS credentials',
    )
    parser.add_argument(
        '--prefix',
        metavar='prefix',
        default='',
        nargs='?',
        help='Prefix of keys in the bucket, often with a trailing slash\n'
             'e.g. my-folder/')
    parser.add_argument(
        '--disable-ssl-verification',
        metavar='',
        nargs='?', const=True, default=False)
    parser.add_argument(
        '--disable-0x20-dns-encoding',
        metavar='',
        nargs='?', const=True, default=False)
    parser.add_argument(
        '--log-level',
        metavar='',
        nargs='?', const=True, default='WARNING')

    parsed_args = parser.parse_args()

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(parsed_args.log_level)
    logger = logging.getLogger()
    logger.setLevel(parsed_args.log_level)
    logger.addHandler(stdout_handler)

    async def transform_fqdn_no_0x20_encoding(fqdn):
        return fqdn

    def get_ssl_context_without_verifcation():
        ssl_context = ssl.SSLContext()
        ssl_context.verify_mode = ssl.CERT_NONE
        return ssl_context

    pool_args = {
        **({
            'get_dns_resolver': lambda **kwargs: Resolver(**{
                **kwargs,
                'transform_fqdn': transform_fqdn_no_0x20_encoding,
            }),
        } if parsed_args.disable_0x20_dns_encoding else {}),
        **({
            'get_ssl_context': get_ssl_context_without_verifcation,
        } if parsed_args.disable_ssl_verification else {}),
    }

    creds_source = parsed_args.credentials_source
    syncer_args = {
        'directory': parsed_args.directory,
        'bucket': parsed_args.bucket,
        'prefix': parsed_args.prefix,
        'region': parsed_args.region,
        'get_pool': lambda: Pool(**pool_args),
        'get_credentials':
            get_credentials_from_environment if creds_source == 'envrionment-variables' else
            get_credentials_from_ecs_endpoint()
    }

    loop = asyncio.get_event_loop()
    cleanup = loop.run_until_complete(async_main(syncer_args))

    async def cleanup_then_stop():
        await cleanup()
        loop.stop()

    def run_cleanup_then_stop():
        loop.create_task(cleanup_then_stop())

    loop.add_signal_handler(signal.SIGINT, run_cleanup_then_stop)
    loop.add_signal_handler(signal.SIGTERM, run_cleanup_then_stop)
    loop.run_forever()


if __name__ == '__main__':
    main()
