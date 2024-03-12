import array
import argparse
import asyncio
from collections import (
    defaultdict,
)
import ctypes
import contextlib
import datetime
import enum
import fcntl
import hashlib
import hmac
import termios
import json
import logging
import os
import re
import signal
import ssl
import stat
import sys
import time
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

import httpx
from fifolock import (
    FifoLock,
)

import sentry_sdk
from sentry_sdk.integrations.httpx import HttpxIntegration


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
    IN_ATTRIB = 0x00000004
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


WATCH_MASK = \
    InotifyEvents.IN_MODIFY | \
    InotifyEvents.IN_ATTRIB | \
    InotifyEvents.IN_CLOSE_WRITE | \
    InotifyEvents.IN_MOVED_FROM | \
    InotifyEvents.IN_MOVED_TO | \
    InotifyEvents.IN_CREATE | \
    InotifyEvents.IN_DELETE | \
    InotifyFlags.IN_ONLYDIR

# We watch the download directly only for moves to be able to use the cookie
# to determine if a move is from a download so we then don't re-upload it
DOWNLOAD_WATCH_MASK = \
    InotifyEvents.IN_MOVED_FROM

EVENT_HEADER = struct.Struct('iIII')


get_current_task = \
    asyncio.current_task if hasattr(asyncio, 'current_task') else \
    asyncio.Task.current_task


async def empty_async_iterator():
    while False:
        yield


async def streamed(data):
    yield data


async def buffered(data):
    return b''.join([chunk async for chunk in data])


@contextlib.contextmanager
def timeout(loop, max_time):

    cancelling_due_to_timeout = False
    current_task = get_current_task()

    def cancel():
        nonlocal cancelling_due_to_timeout
        cancelling_due_to_timeout = True
        current_task.cancel()

    def reset():
        nonlocal handle
        handle.cancel()
        handle = loop.call_later(max_time, cancel)

    handle = loop.call_later(max_time, cancel)

    try:
        yield reset
    except asyncio.CancelledError:
        if cancelling_due_to_timeout:
            raise asyncio.TimeoutError()
        raise
    finally:
        handle.cancel()


def Pool(
        get_ssl_context=ssl.create_default_context,
        get_logger_adapter=get_logger_adapter_default,
        force_ipv4=False,
    ):

    logger = get_logger_adapter({})

    async def log_request(request):
        logger.info('[http] Request: %s %s', request.method, request.url)

    async def log_response(response):
        logger.info('[http] Response: %s %s %s', response.request.method, response.request.url, response.status_code)

    return httpx.AsyncClient(
        timeout=10.0,
        transport=httpx.AsyncHTTPTransport(
            retries=3,
            verify=get_ssl_context(),
            local_address="0.0.0.0" if force_ipv4 else None,
        ),
        event_hooks={'request': [log_request], 'response': [log_response]}
    )


def AWSAuth(service, region, client, get_credentials, content_hash=hashlib.sha256().hexdigest()):

    def aws_sigv4_headers(access_key_id, secret_access_key, pre_auth_headers,
                          method, path, params):
        algorithm = 'AWS4-HMAC-SHA256'

        now = datetime.datetime.utcnow()
        amzdate = now.strftime('%Y%m%dT%H%M%SZ')
        datestamp = now.strftime('%Y%m%d')
        credential_scope = f'{datestamp}/{region}/{service}/aws4_request'

        pre_auth_headers_lower = tuple(
            (header_key.lower(), ' '.join(header_value.split()))
            for header_key, header_value in pre_auth_headers
        )
        required_headers = (
            ('x-amz-content-sha256', content_hash),
            ('x-amz-date', amzdate),
        )
        headers = sorted(pre_auth_headers_lower + required_headers)
        signed_headers = ';'.join(key for key, _ in headers)

        def signature():
            def canonical_request():
                canonical_uri = urllib.parse.quote(path, safe='/~')
                quoted_params = sorted(
                    (urllib.parse.quote(key, safe='~'), urllib.parse.quote(value, safe='~'))
                    for key, value in params
                )
                canonical_querystring = '&'.join(f'{key}={value}' for key, value in quoted_params)
                canonical_headers = ''.join(f'{key}:{value}\n' for key, value in headers)

                return f'{method}\n{canonical_uri}\n{canonical_querystring}\n' + \
                       f'{canonical_headers}\n{signed_headers}\n{content_hash}'

            def sign(key, msg):
                return hmac.new(key, msg.encode('ascii'), hashlib.sha256).digest()

            string_to_sign = f'{algorithm}\n{amzdate}\n{credential_scope}\n' + \
                             hashlib.sha256(canonical_request().encode('ascii')).hexdigest()

            date_key = sign(('AWS4' + secret_access_key).encode('ascii'), datestamp)
            region_key = sign(date_key, region)
            service_key = sign(region_key, service)
            request_key = sign(service_key, 'aws4_request')
            return sign(request_key, string_to_sign).hex()

        return (
            ('authorization', (
                f'{algorithm} Credential={access_key_id}/{credential_scope}, '
                f'SignedHeaders={signed_headers}, Signature=' + signature())
             ),
            ('x-amz-date', amzdate),
            ('x-amz-content-sha256', content_hash),
        )

    class _AWSAuth(httpx.Auth):
        async def async_auth_flow(self, request):
            access_key_id, secret_access_key, auth_headers = await get_credentials(client)

            params = tuple((key.decode(), value.decode()) for (key, value) in urllib.parse.parse_qsl(request.url.query, keep_blank_values=True))
            existing_headers = tuple((key, value) for (key, value) in request.headers.items() if key == 'host' or key.startswith('content-') or key.startswith('x-amz-'))

            headers_to_set = aws_sigv4_headers(
                access_key_id, secret_access_key, existing_headers + auth_headers,
                request.method, request.url.path, params,
            ) + auth_headers
            for key, value in headers_to_set:
                request.headers[key] = value

            yield request

    return _AWSAuth()


async def get_credentials_from_environment(_):
    return os.environ['AWS_ACCESS_KEY_ID'], os.environ['AWS_SECRET_ACCESS_KEY'], ()


def get_credentials_from_ecs_endpoint():
    aws_access_key_id = None
    aws_secret_access_key = None
    pre_auth_headers = None
    expiration = datetime.datetime.fromtimestamp(0)

    async def _get_credentials(client):
        nonlocal aws_access_key_id
        nonlocal aws_secret_access_key
        nonlocal pre_auth_headers
        nonlocal expiration

        now = datetime.datetime.now()

        if now > expiration:
            response = await client.request(
                b'GET',
                'http://169.254.170.2' + os.environ['AWS_CONTAINER_CREDENTIALS_RELATIVE_URI']
            )
            creds = json.loads(response.content)
            aws_access_key_id = creds['AccessKeyId']
            aws_secret_access_key = creds['SecretAccessKey']
            expiration = datetime.datetime.strptime(creds['Expiration'], '%Y-%m-%dT%H:%M:%SZ')
            pre_auth_headers = (
                ('x-amz-security-token', creds['Token']),
            )

        return aws_access_key_id, aws_secret_access_key, pre_auth_headers

    return _get_credentials


def Syncer(
        directory, bucket, endpoint, region,
        prefix='',
        concurrent_uploads=5,
        concurrent_downloads=5,
        get_credentials=get_credentials_from_environment,
        get_pool=Pool,
        flush_file_root='.__mobius3_flush__',
        flush_file_timeout=5,
        directory_watch_timeout=5,
        download_directory='.mobius3',
        get_logger_adapter=get_logger_adapter_default,
        local_modification_persistance=120,
        download_interval=10,
        exclude_remote=r'^$',
        exclude_local=r'^$',
        upload_on_create=r'^$',
        cloudwatch_monitoring_endpoint='',
        cloudwatch_monitoring_region='',
        cloudwatch_monitoring_namespace='',
):

    loop = asyncio.get_running_loop()
    logger = get_logger_adapter({})

    directory = PurePosixPath(directory)
    exclude_remote = re.compile(exclude_remote)
    exclude_local = re.compile(exclude_local)
    upload_on_create = re.compile(upload_on_create)
    bucket_url = endpoint.format(bucket)

    # The file descriptor returned from inotify_init
    fd = None

    # Watch descriptors to paths. A notification returns only a relative
    # path to its watch descriptor path: these are used to find the full
    # path of any notified-on files
    wds_to_path = {}

    # To migitate (but not eliminate) the chance that nested files are
    # immediately re-uploaded
    directory_watch_events = WeakValueDictionary()

    # The asyncio task pool that performs the uploads
    upload_tasks = []

    # PUTs and DELETEs are initiated in the order generated by inotify events
    upload_job_queue = asyncio.Queue()

    download_manager_task = None
    download_tasks = []
    download_job_queue = asyncio.Queue()

    # To prevent concurrent HTTP requests on the same files where order of
    # receipt by S3 cannot be guaranteed, we wrap each request by a lock
    # e.g. to prevent a DELETE overtaking a PUT
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

    # During a queue of a PUT or DELETE, and for
    # local_modification_persistance seconds, we do not overwrite local files
    # with information from S3 for eventual consistency reasons
    push_queued = defaultdict(int)
    push_completed = ExpiringSet(loop, local_modification_persistance)

    # When moving download files from the hidden directory to their final
    # position, we would like to detect if this is indeed a move from a
    # download (in which case we don't re-upload), or a real move from
    download_cookies = ExpiringSet(loop, 10)

    # When we delete a file locally, do not attempt to delete it remotely
    ignore_next_delete = {}

    # When we add a directory from a download, do not attempt to reupload
    ignore_next_directory_upload = {}

    # When downloading a file, we note its etag. We don't re-download it later
    # if the etag matches
    etags = {}

    # Don't re-upload metadata if we think S3 already has it
    meta = {}

    # A cache of the file tree is maintained. Used for directory renames: we
    # only get notified of renames _after_ they have happened, we need a way
    # to know what objects are on S3 in order to DELETE them
    tree_cache_root = {
        'type': 'directory',
        'children': {},
    }

    client = get_pool()
    auth = AWSAuth(service='s3', region=region, client=client, get_credentials=get_credentials, content_hash='UNSIGNED-PAYLOAD')

    def ensure_file_in_tree_cache(path):
        parent_dir = ensure_parent_dir_in_tree_cache(path)
        parent_dir['children'][path.name] = {
            'type': 'file',
        }

    def ensure_dir_in_tree_cache(path):
        parent_dir = ensure_parent_dir_in_tree_cache(path)
        parent_dir['children'].setdefault(path.name, {
            'type': 'directory',
            'children': {},
        })

    def ensure_parent_dir_in_tree_cache(path):
        directory = tree_cache_root
        for parent in reversed(list(path.parents)):
            directory = directory['children'].setdefault(parent.name, {
                'type': 'directory',
                'children': {},
            })
        return directory

    def remove_from_tree_cache(path):
        directory = tree_cache_root
        for parent in reversed(list(path.parents)):
            directory = directory['children'][parent.name]
        del directory['children'][path.name]

    def tree_cache_directory(path):
        directory = tree_cache_root
        for parent in reversed(list(path.parents)):
            directory = directory['children'][parent.name]
        return directory['children'][path.name]

    def set_etag(path, headers):
        etags[path] = headers['etag']

    def queued_push_local_change(path):
        push_queued[path] += 1

    def completed_push_local_change(path):
        push_queued[path] -= 1
        if push_queued[path] == 0:
            del push_queued[path]
        push_completed.add(path)

    def is_dir_pull_blocked(path):
        return path in push_queued or path in push_completed

    def is_pull_blocked(path):
        # For extremely recent modifications we may not have yielded the event
        # loop to add files to the queue. We do our best and check the mtime
        # to prevent overriding with older remote data. However, after we
        # check a file could still be modified locally, and we have no way to
        # detect this

        def modified_recently():
            now = datetime.datetime.now().timestamp()
            try:
                return now - os.lstat(path).st_mtime < local_modification_persistance
            except FileNotFoundError:
                return False

        return path in push_queued or path in push_completed or modified_recently()

    async def start():
        logger = get_logger_adapter({'mobius3_component': 'start'})
        logger.info('Starting')
        logger.info('Excluding: %s', exclude_remote)
        nonlocal upload_tasks
        nonlocal download_tasks
        nonlocal download_manager_task
        start_time = time.monotonic()
        try:
            os.mkdir(directory / download_directory)
        except FileExistsError:
            pass
        upload_tasks = [
            asyncio.create_task(process_jobs(upload_job_queue))
            for i in range(0, concurrent_uploads)
        ]
        download_tasks = [
            asyncio.create_task(process_jobs(download_job_queue))
            for i in range(0, concurrent_downloads)
        ]
        start_inotify(logger, upload=False)
        await list_and_schedule_downloads(logger)
        download_manager_task = asyncio.create_task(
            download_manager(get_logger_adapter({'mobius3_component': 'download'}))
        )
        end_time = time.monotonic()
        logger.info('Finished starting: %s seconds', end_time - start_time)
        if cloudwatch_monitoring_endpoint and cloudwatch_monitoring_region and cloudwatch_monitoring_namespace:
            logger.info('Posting startup time to %s in region %s in namespace %s', cloudwatch_monitoring_endpoint, cloudwatch_monitoring_region, cloudwatch_monitoring_namespace)
            cloudwatch_auth = AWSAuth(service='monitoring', region=cloudwatch_monitoring_region, client=client, get_credentials=get_credentials)
            response = await client.post(cloudwatch_monitoring_endpoint, params=(
                ('Version', '2010-08-01'),
                ('Action', 'PutMetricData'),
                ('Namespace', cloudwatch_monitoring_namespace),
                ('MetricData.member.1.MetricName', 'StartupSyncTime'),
                ('MetricData.member.1.Unit', 'Seconds'),
                ('MetricData.member.1.Value', str(end_time - start_time)),
            ), auth=cloudwatch_auth)
            response.raise_for_status()

    def start_inotify(logger, upload):
        nonlocal wds_to_path
        nonlocal meta
        nonlocal etags
        nonlocal tree_cache_root
        nonlocal fd
        nonlocal ignore_next_delete
        nonlocal ignore_next_directory_upload
        ignore_next_delete = {}
        ignore_next_directory_upload = {}
        wds_to_path = {}
        meta = {}
        etags = {}
        tree_cache_root = {
            'type': 'directory',
            'children': {},
        }
        fd = call_libc(libc.inotify_init)

        def _read_events():
            logger = get_logger_adapter({'mobius3_component': 'event'})
            read_events(logger)

        loop.add_reader(fd, _read_events)
        watch_directory(download_directory, DOWNLOAD_WATCH_MASK)
        watch_directory_recursive(logger, directory, WATCH_MASK, upload)

    async def stop():
        async def cancel(task):
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Make every effort to read all incoming events and finish the queue
        logger = get_logger_adapter({'mobius3_component': 'stop'})
        logger.info('Stopping')
        if download_manager_task is not None:
            await cancel(download_manager_task)
        for task in download_tasks:
            await cancel(task)
        read_events(logger)
        while upload_job_queue._unfinished_tasks:
            await upload_job_queue.join()
            read_events(logger)
        stop_inotify()
        for task in upload_tasks:
            await cancel(task)
        await client.aclose()
        await asyncio.sleep(0)
        logger.info('Finished stopping')

    def stop_inotify():
        loop.remove_reader(fd)
        os.close(fd)

    def watch_directory(path, mask):
        try:
            wd = call_libc(libc.inotify_add_watch, fd, str(path).encode('utf-8'), mask)
        except (NotADirectoryError, FileNotFoundError):
            return

        # After a directory rename, we will be changing the path of an
        # existing entry, but that's fine
        wds_to_path[wd] = path

        # Notify any waiting watchers
        directory_watch_events.setdefault(path, default=asyncio.Event()).set()

    def watch_directory_recursive(logger, path, mask, upload):
        logger.info('Watching directory: %s, with upload: %s', path, upload)
        watch_directory(path, mask)

        if PurePosixPath(path) not in [directory, directory / download_directory]:
            try:
                del ignore_next_directory_upload[path]
            except KeyError:
                if upload:
                    logger.info('Scheduling upload directory: %s', path)
                    schedule_upload_directory(logger, path)

        # By the time we've added a watcher, files or subdirectories may have
        # already been created
        for root, dirs, files in os.walk(path):
            if PurePosixPath(root) == directory / download_directory:
                continue
            if upload:
                for file in files:
                    logger.info('Scheduling upload: %s', PurePosixPath(root) / file)
                    schedule_upload(logger, PurePosixPath(root) / file)

            for d in dirs:
                watch_directory_recursive(logger, PurePosixPath(root) / d, mask, upload)

    def remote_delete_directory(logger, path):
        # Directory nesting not likely to be large
        def recursive_delete(prefix, directory):
            for child_name, child in list(directory['children'].items()):
                if child['type'] == 'file':
                    logger.info('Scheduling delete: %s', prefix / child_name)
                    schedule_delete(logger, prefix / child_name)
                else:
                    recursive_delete(prefix / child_name, child)
                    schedule_delete_directory(logger, prefix / child_name)

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
            schedule_delete_directory(logger, path)

    def read_events(parent_logger):
        FIONREAD_output = array.array('i', [0])
        fcntl.ioctl(fd, termios.FIONREAD, FIONREAD_output)
        bytes_to_read = FIONREAD_output[0]

        if not bytes_to_read:
            return
        raw_bytes = os.read(fd, bytes_to_read)

        offset = 0
        while offset < len(raw_bytes):
            wd, mask, cookie, length = EVENT_HEADER.unpack_from(raw_bytes, offset)
            offset += EVENT_HEADER.size
            path = PurePosixPath(raw_bytes[offset:offset+length].rstrip(b'\0').decode('utf-8'))
            offset += length

            event_id = uuid.uuid4().hex[:8]
            logger = child_adapter(parent_logger, {'event': event_id})

            full_path = \
                wds_to_path[wd] / path if wd != -1 else \
                directory  # Overflow event

            events = [event for event in InotifyEvents.__members__.values() if event & mask]
            item_type = \
                'overflow' if mask & InotifyEvents.IN_Q_OVERFLOW else \
                'flush' if path.name.startswith(flush_file_root) and full_path in flushes else \
                'download' if full_path.parent == directory / download_directory else \
                'dir' if mask & InotifyFlags.IN_ISDIR else \
                'file'
            for event in events:
                handler_name = f'handle__{item_type}__{event.name}'
                try:
                    handler = parent_locals[handler_name]
                except KeyError:
                    continue
                else:
                    logger.debug('Path: %s', full_path)
                    logger.debug('Handler: %s', handler_name)

                try:
                    handler(logger, wd, cookie, full_path)
                except Exception:
                    logger.exception('Exception calling handler')

    def handle__overflow__IN_Q_OVERFLOW(logger, _, __, ___):
        logger.warning('IN_Q_OVERFLOW. Restarting')
        stop_inotify()
        start_inotify(logger, upload=True)

    def handle__flush__IN_CREATE(logger, _, __, path):
        flush = flushes[path]
        logger.debug('Flushing: %s', path)
        flush.set()

    def handle__download__IN_MOVED_FROM(logger, __, cookie, ___):
        logger.debug('Cookie: %s', cookie)
        download_cookies.add(cookie)

    def handle__file__IN_ATTRIB(logger, _, __, path):
        schedule_upload_meta(logger, path)

    def handle__file__IN_CLOSE_WRITE(logger, _, __, path):
        schedule_upload(logger, path)

    def handle__file__IN_CREATE(logger, _, __, path):
        if upload_on_create.match(str(path)) or os.path.islink(path):
            schedule_upload(logger, path)

    def handle__dir__IN_CREATE(logger, _, __, path):
        watch_directory_recursive(logger, path, WATCH_MASK, upload=True)

    def handle__file__IN_DELETE(logger, _, __, path):
        try:
            del etags[path]
        except KeyError:
            pass

        try:
            del meta[path]
        except KeyError:
            pass

        try:
            del ignore_next_delete[path]
        except KeyError:
            pass
        else:
            return

        # Correctness does not depend on this bump: it's an optimisation
        # that ensures we abandon any upload of this path ahead of us
        # in the queue
        bump_content_version(path)
        schedule_delete(logger, path)

    def handle__dir__IN_DELETE(logger, _, __, path):
        try:
            del ignore_next_delete[path]
        except KeyError:
            pass
        else:
            return

        schedule_delete_directory(logger, path)

    def handle__file__IN_IGNORED(_, wd, __, ___):
        # For some reason IN_ISDIR is not set with IN_IGNORED
        del wds_to_path[wd]

    def handle__file__IN_MODIFY(_, __, ___, path):
        bump_content_version(path)

    def handle__dir__IN_MOVED_FROM(logger, _, __, path):
        remote_delete_directory(logger, path)

    def handle__file__IN_MOVED_FROM(logger, _, __, path):
        try:
            del etags[path]
        except KeyError:
            pass
        schedule_delete(logger, path)

    def handle__dir__IN_MOVED_TO(logger, _, __, path):
        watch_directory_recursive(logger, path, WATCH_MASK, upload=True)

    def handle__file__IN_MOVED_TO(logger, _, cookie, path):
        bump_content_version(path)
        if cookie in download_cookies:
            logger.debug('Cookie: %s', cookie)
            return
        schedule_upload(logger, path)

    def get_content_version(path):
        return content_versions.setdefault(path, default=WeakReferenceableDict(version=0))

    def bump_content_version(path):
        get_content_version(path)['version'] += 1

    def get_lock(path):
        return path_locks.setdefault(path, default=FifoLock())

    def schedule_upload_meta(logger, path):
        if exclude_local.match(str(path)):
            logger.info('Excluding from upload: %s', path)
            return

        version_current = get_content_version(path)
        version_original = version_current.copy()

        async def function():
            try:
                await upload_meta(logger, path, version_current, version_original)
            finally:
                completed_push_local_change(path)

        ensure_file_in_tree_cache(path)
        upload_job_queue.put_nowait((logger, function))
        queued_push_local_change(path)

    def schedule_upload(logger, path):
        if exclude_local.match(str(path)):
            logger.info('Excluding from upload: %s', path)
            return

        version_current = get_content_version(path)
        version_original = version_current.copy()

        async def function():
            try:
                await upload(logger, path, version_current, version_original)
            finally:
                completed_push_local_change(path)

        ensure_file_in_tree_cache(path)
        upload_job_queue.put_nowait((logger, function))
        queued_push_local_change(path)

    def schedule_upload_directory(logger, path):
        if exclude_local.match(str(path)):
            logger.info('Excluding from upload: %s', path)
            return

        async def function():
            try:
                await upload_directory(logger, path)
            finally:
                completed_push_local_change(path)

        ensure_dir_in_tree_cache(path)
        upload_job_queue.put_nowait((logger, function))
        queued_push_local_change(path)

    def schedule_delete(logger, path):
        if exclude_local.match(str(path)):
            logger.info('Excluding from delete: %s', path)
            return

        version_current = get_content_version(path)
        version_original = version_current.copy()

        async def function():
            try:
                await delete(logger, path, version_current, version_original)
            finally:
                completed_push_local_change(path)

        try:
            remove_from_tree_cache(path)
        except KeyError:
            pass
        upload_job_queue.put_nowait((logger, function))
        queued_push_local_change(path)

    def schedule_delete_directory(logger, path):
        if exclude_local.match(str(path)):
            logger.info('Excluding from delete: %s', path)
            return

        async def function():
            try:
                await delete_directory(logger, path)
            finally:
                completed_push_local_change(path)

        try:
            remove_from_tree_cache(path)
        except KeyError:
            pass

        upload_job_queue.put_nowait((logger, function))
        queued_push_local_change(path)

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
                    logger.info('Content changed, aborting: %s', exception)
                if (
                        isinstance(exception, FileNotFoundError) or
                        isinstance(exception.__cause__, FileNotFoundError)
                ):
                    logger.info('File not found: %s', exception)
                if (
                        not isinstance(exception, FileNotFoundError) and
                        not isinstance(exception.__cause__, FileNotFoundError) and
                        not isinstance(exception, FileContentChanged) and
                        not isinstance(exception.__cause__, FileContentChanged)
                ):
                    logger.exception('Exception during %s', job)
            finally:
                queue.task_done()

    async def flush_events(logger, path):
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

    async def wait_for_directory_watched(path):
        # Inneficient search
        if path.parent in wds_to_path.values():
            return

        event = directory_watch_events.setdefault(path.parent, default=asyncio.Event())
        with timeout(loop, directory_watch_timeout):
            await event.wait()

    async def upload(logger, path, content_version_current, content_version_original):
        logger.info('Uploading %s', path)

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
                        await flush_events(logger, path)

                    if content_version_current != content_version_original:
                        raise FileContentChanged(path)

                    yield chunk

        async def symlink_points_to():
            yield os.readlink(path).encode('utf-8')

        is_symlink = os.path.islink(path)

        if not is_symlink:
            content = file_body()
            content_length = str(os.stat(path).st_size)
        else:
            content = symlink_points_to()
            content_length = str(len(os.readlink(path).encode('utf-8')))

        mtime = str(os.lstat(path).st_mtime)
        mode = str(os.lstat(path).st_mode)

        # Ensure we only progress if the content length hasn't changed since
        # we have queued the upload
        await flush_events(logger, path)
        if content_version_current != content_version_original:
            raise FileContentChanged(path)

        data = (
            ('x-amz-meta-mtime', mtime),
            ('x-amz-meta-mode', mode),
        )

        def set_etag_and_meta(path, headers):
            meta[path] = data
            set_etag(path, headers)

        await locked_request(
            logger, b'PUT', path, file_key_for_path(path), content=content,
            get_headers=lambda: (
                ('content-length', content_length),
            ) + data,
            on_done=set_etag_and_meta,
        )

    async def upload_meta(logger, path, content_version_current, content_version_original):
        logger.info('Uploading meta %s', path)

        mtime = str(os.lstat(path).st_mtime)
        mode = str(os.lstat(path).st_mode)

        # Ensure we only progress if the content hasn't changed since we have
        # queued the upload
        await flush_events(logger, path)
        if content_version_current != content_version_original:
            raise FileContentChanged(path)

        data = (
            ('x-amz-meta-mtime', mtime),
            ('x-amz-meta-mode', mode),
        )

        def set_meta(path, _):
            meta[path] = data

        key = file_key_for_path(path)
        await locked_request(
            logger, b'PUT', path, key,
            cont=lambda: meta[path] != data,
            get_headers=lambda: data + (
                ('x-amz-copy-source', f'/{bucket}/{key}'),
                ('x-amz-metadata-directive', 'REPLACE'),
                ('x-amz-copy-source-if-match', etags[path]),
            ),
            on_done=set_meta,
        )

    async def upload_directory(logger, path):
        logger.info('Uploading directory %s', path)

        mtime = str(os.stat(path).st_mtime)

        if not os.path.isdir(path):
            raise FileContentChanged(path)

        await locked_request(
            logger, b'PUT', path, dir_key_for_path(path),
            get_headers=lambda: (
                ('content-length', '0'),
                ('x-amz-meta-mtime', mtime),
            ),
            on_done=set_etag,
        )

    async def delete(logger, path, content_version_current, content_version_original):
        logger.info('Deleting %s', path)

        # We may have recently had an download from S3, so we don't carry on
        # with the DELETE (if we did, we would then delete the local file
        # on the next download)
        try:
            await flush_events(logger, path)
        except FileNotFoundError:
            # The local folder in which the file was may have been deleted,
            # but we still want to carry on with the remote delete
            pass

        if content_version_current != content_version_original:
            raise FileContentChanged(path)

        await locked_request(logger, b'DELETE', path, file_key_for_path(path))

    async def delete_directory(logger, path):
        logger.info('Deleting directory %s', path)

        if os.path.isdir(path):
            raise FileContentChanged(path)

        await locked_request(logger, b'DELETE', path, dir_key_for_path(path))

    def file_key_for_path(path):
        return prefix + str(path.relative_to(directory))

    def dir_key_for_path(path):
        return prefix + str(path.relative_to(directory)) + '/'

    async def locked_request(logger, method, path, key, cont=lambda: True,
                             get_headers=lambda: (),
                             content=empty_async_iterator(),
                             on_done=lambda path, headers: None):
        # Keep a reference to the lock to keep it in the WeakValueDictionary
        lock = get_lock(path)
        async with lock(Mutex):
            if not cont():
                return
            remote_url = bucket_url + key
            headers = get_headers()
            logger.debug('%s %s %s', method.decode(), remote_url, headers)
            response = await client.request(method, remote_url, headers=get_headers(), content=content, auth=auth)
            logger.debug('%s %s', response.status_code, response.headers)

            if response.status_code not in [200, 204]:
                raise Exception(status_code, content)

            on_done(path, response.headers)

    async def download_manager(logger):
        while True:
            try:
                await list_and_schedule_downloads(logger)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception('Failed to list files')
            await asyncio.sleep(download_interval)

    async def list_and_schedule_downloads(logger):
        logger.debug('Listing keys')

        path_etags = [
            (path, etag) async for path, etag in list_keys_relative_to_prefix(logger)
        ]

        for path, etag in path_etags:
            try:
                etag_existing = etags[directory / path]
            except KeyError:
                pass
            else:
                if etag == etag_existing:
                    logger.debug('Existing etag matches for: %s', path)
                    continue

            logger.info('Scheduling download: %s', path)
            schedule_download(logger, path)

        await download_job_queue.join()

        full_paths = set(directory / path for path, _ in path_etags)
        full_paths_all_parents = set(
            parent
            for path, _ in path_etags
            for parent in (directory / path).parents
        )
        for root, dirs, files in os.walk(directory, topdown=False):
            for file in files:
                full_path = PurePosixPath(root) / file

                if (
                        exclude_local.match(str(full_path)) or
                        full_path in full_paths or
                        is_pull_blocked(full_path)
                ):
                    continue

                # Since walking the filesystem can take time we might have a new file that we have
                # recently uploaded that was not present when we request the original file list.
                path = full_path.relative_to(directory)
                response = await client.request(b'HEAD', bucket_url + prefix + str(path), auth=auth)
                if response.status_code != 404:
                    continue

                # Check again if we have made modifications since the above request can take time
                try:
                    await flush_events(logger, full_path)
                except (FileNotFoundError, OSError):
                    continue
                if is_pull_blocked(full_path):
                    continue

                try:
                    logger.info('Deleting locally %s', full_path)
                    os.remove(full_path)
                except (FileNotFoundError, OSError):
                    pass
                else:
                    # The remove will queue a remote DELETE. However, the file already doesn't
                    # appear to exist in S3, so a) there is no need and b) may actually delete
                    # data either added by another client, or even from this one in the case
                    # of an extremely long eventual consistency issue where a PUT of a object
                    # that did not previously exist is still appearing
                    ignore_next_delete[full_path] = True

            for dir_ in dirs:
                full_path = PurePosixPath(root) / dir_
                if (
                        exclude_local.match(str(full_path)) or
                        full_path in full_paths or
                        full_path in full_paths_all_parents or
                        is_dir_pull_blocked(full_path) or
                        full_path == directory / download_directory
                ):
                    continue

                path = full_path.relative_to(directory)
                response = await client.request(b'HEAD', bucket_url + prefix + str(path) + '/', auth=auth)
                if response.status_code != 404:
                    continue

                try:
                    await flush_events(logger, full_path)
                except (FileNotFoundError, OSError):
                    continue
                if is_dir_pull_blocked(full_path):
                    continue

                try:
                    logger.info('Deleting locally %s', full_path)
                    os.rmdir(full_path)
                except (FileNotFoundError, OSError):
                    pass
                else:
                    ignore_next_delete[full_path] = True

    def schedule_download(logger, path):
        async def download():
            full_path = directory / path

            if is_pull_blocked(full_path):
                logger.debug('Recently changed locally, not changing: %s', full_path)
                return

            logger.info('Downloading: %s', full_path)

            async with client.stream(b'GET', bucket_url + prefix + path, auth=auth) as response:
                if response.status_code != 200:
                    await buffered(response.aiter_bytes())  # Fetch all bytes and return to pool
                    raise Exception(response.status_code)

                is_directory = path[-1] == '/'

                directory_to_ensure_created = \
                    full_path if is_directory else \
                    full_path.parent

                # Create directories under directory
                directory_and_parents = [directory] + list(directory.parents)
                directory_to_ensure_created_and_paraents = list(
                    reversed(directory_to_ensure_created.parents)) + [directory_to_ensure_created]
                directories_to_ensure_created_under_directory = [
                    _dir
                    for _dir in directory_to_ensure_created_and_paraents
                    if _dir not in directory_and_parents
                ]
                for _dir in directories_to_ensure_created_under_directory:

                    # If we don't wait for the containing directory to be watched,
                    # then we might be incorrectly ignoring
                    await wait_for_directory_watched(_dir)

                    try:
                        os.mkdir(_dir)
                    except FileExistsError:
                        logger.debug('Already exists: %s', _dir)
                    except NotADirectoryError:
                        logger.debug('Not a directory: %s', _dir)
                    except Exception:
                        logger.debug('Unable to create directory: %s', _dir)
                    else:
                        ignore_next_directory_upload[_dir] = True

                try:
                    modified = float(response.headers['x-amz-meta-mtime'])
                except (KeyError, ValueError):
                    modified = datetime.datetime.strptime(
                        response.headers['last-modified'],
                        '%a, %d %b %Y %H:%M:%S %Z').timestamp()

                try:
                    mode = int(response.headers['x-amz-meta-mode'])
                except (KeyError, ValueError):
                    mode = None

                is_symlink = mode and stat.S_ISLNK(mode)

                if is_directory:
                    await buffered(response.aiter_bytes())

                    if is_dir_pull_blocked(full_path):
                        logger.debug('Recently changed locally, not changing: %s', full_path)
                        return

                    os.utime(full_path, (modified, modified))
                    etags[full_path] = response.headers['etag']

                    # Ensure that subsequent renames will attempt to move the directory
                    ensure_dir_in_tree_cache(full_path)
                    return

                temporary_path = directory / download_directory / uuid.uuid4().hex
                try:
                    if not is_symlink:
                        with open(temporary_path, 'wb') as file:
                            async for chunk in response.aiter_bytes():
                                file.write(chunk)
                    else:
                        symlink_points_to = await buffered(response.aiter_bytes())
                        os.symlink(symlink_points_to, temporary_path)

                    # May raise a FileNotFoundError if the directory no longer
                    # exists, but handled at higher level
                    os.utime(temporary_path, (modified, modified), follow_symlinks=False)

                    # This is skipped when the file being downloaded is a symlink because it
                    # will fail if the symlink is processed before the file it points to exists.
                    # Permissions on symlinks in Linux are always 777 anyway as it defers to
                    # the permissions of the underlying file
                    if mode is not None and not is_symlink:
                        os.chmod(temporary_path, mode)

                    # If we don't wait for the directory watched, then if
                    # - a directory has just been created above
                    # - a download that doesn't yield (enough) for the create
                    #   directory eveny to have been processed
                    # once the IN_CREATE event for the directory is processed it
                    # would discover the file and re-upload
                    await wait_for_directory_watched(full_path)

                    try:
                        await flush_events(logger, full_path)
                    except FileNotFoundError:
                        # The folder doesn't exist, so moving into place will fail
                        return

                    if is_pull_blocked(full_path):
                        logger.debug('Recently changed locally, not changing: %s', full_path)
                        return

                    os.replace(temporary_path, full_path)

                    meta[full_path] = (
                        (b'x-amz-meta-mtime', modified),
                        (b'x-amz-meta-mode', mode),
                    )

                    # Ensure that once we move the file into place, subsequent
                    # renames will attempt to move the file
                    ensure_file_in_tree_cache(full_path)
                finally:
                    try:
                        os.remove(temporary_path)
                    except FileNotFoundError:
                        pass
                etags[full_path] = response.headers['etag']

        download_job_queue.put_nowait((logger, download))

    async def list_keys_relative_to_prefix(logger):
        async def _list(extra_query_items=()):
            query = (
                ('max-keys', '1000'),
                ('list-type', '2'),
                ('prefix', prefix),
            ) + extra_query_items
            response = await client.request(b'GET', bucket_url, params=query, auth=auth)
            if response.status_code != 200:
                raise Exception(response.status_code, response.content)

            namespace = '{http://s3.amazonaws.com/doc/2006-03-01/}'
            root = ET.fromstring(response.content)
            next_token = ''
            keys_relative = []
            for element in root:
                if element.tag == f'{namespace}Contents':
                    key = first_child_text(element, f'{namespace}Key')
                    key_relative = key[len(prefix):]
                    if key_relative == '':
                        # Don't include the prefix itself, if it exists
                        continue
                    if exclude_remote.match(key_relative):
                        logger.info('Excluding: %s', key_relative)
                        continue
                    etag = first_child_text(element, f'{namespace}ETag')
                    keys_relative.append((key_relative, etag))
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


class ExpiringDict:

    def __init__(self, loop, seconds):
        self._loop = loop
        self._seconds = seconds
        self._store = {}

    def __getitem__(self, key):
        return self._store[key][0]

    def __setitem__(self, key, value):
        def delete():
            del self._store[key]

        if key in self._store:
            self._store[key][1].cancel()
            del self._store[key]

        delete_handle = self._loop.call_later(self._seconds, delete)
        self._store[key] = (value, delete_handle)

    def __contains__(self, key):
        return key in self._store


class ExpiringSet:

    def __init__(self, loop, seconds):
        self._loop = loop
        self._store = ExpiringDict(loop, seconds)

    def add(self, item):
        self._store[item] = True

    def __contains__(self, item):
        return item in self._store


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
        'endpoint',
        metavar='endpoint',
        help='Pattern which is filled with the bucket name\n'
             'e.g. https://s3-eu-west-2.amazonaws.com/{}/')
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
        '--exclude-remote',
        metavar='exclude-remote',
        default='^$',
        nargs='?',
        help='Regex of keys to not be downloaded')
    parser.add_argument(
        '--exclude-local',
        metavar='exclude-local',
        default='^$',
        nargs='?',
        help='Regex of paths to not be uploaded')
    parser.add_argument(
        '--upload-on-create',
        metavar='upload-on-create',
        default='^$',
        nargs='?',
        help='Regex of paths to upload as soon as they have been created')

    parser.add_argument(
        '--cloudwatch-monitoring-endpoint',
        metavar='cloudwatch-monitoring-endpoint',
        default='',
        nargs='?',
        help='The endpoint of for CloudWatch monitoring for metrics to be posted to')
    parser.add_argument(
        '--cloudwatch-monitoring-region',
        metavar='cloudwatch-monitoring-region',
        default='',
        nargs='?',
        help='The endpoint of for CloudWatch monitoring for metrics to be posted to')
    parser.add_argument(
        '--cloudwatch-monitoring-namespace',
        metavar='cloudwatch-monitoring-namespace',
        default='',
        nargs='?',
        help='The namespace for CloudWatch monitoring metrics')
    parser.add_argument(
        '--disable-ssl-verification',
        metavar='',
        nargs='?', const=True, default=False)
    parser.add_argument(
        '--log-level',
        metavar='',
        nargs='?', const=True, default='WARNING')
    parser.add_argument(
        '--force-ipv4',
        metavar='',
        nargs='?', const=False, default=False)

    parsed_args = parser.parse_args()

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(parsed_args.log_level)
    logger = logging.getLogger('mobius3')
    logger.setLevel(parsed_args.log_level)
    logger.addHandler(stdout_handler)

    def get_ssl_context_without_verifcation():
        ssl_context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        return ssl_context

    pool_args = {
        'force_ipv4': parsed_args.force_ipv4,
        **({
            'get_ssl_context': get_ssl_context_without_verifcation,
        } if parsed_args.disable_ssl_verification else {}),
    }

    creds_source = parsed_args.credentials_source
    syncer_args = {
        'directory': parsed_args.directory,
        'bucket': parsed_args.bucket,
        'endpoint': parsed_args.endpoint,
        'prefix': parsed_args.prefix,
        'region': parsed_args.region,
        'exclude_remote': parsed_args.exclude_remote,
        'exclude_local': parsed_args.exclude_local,
        'upload_on_create': parsed_args.upload_on_create,
        'cloudwatch_monitoring_endpoint': parsed_args.cloudwatch_monitoring_endpoint,
        'cloudwatch_monitoring_region': parsed_args.cloudwatch_monitoring_region,
        'cloudwatch_monitoring_namespace': parsed_args.cloudwatch_monitoring_namespace,
        'get_pool': lambda: Pool(**pool_args),
        'get_credentials':
            get_credentials_from_environment if creds_source == 'envrionment-variables' else
            get_credentials_from_ecs_endpoint()
    }

    if os.environ.get("SENTRY_DSN") is not None:
        sentry_sdk.init(
            dsn=os.environ["SENTRY_DSN"],
            integrations=[HttpxIntegration()],
            environment=os.environ.get("SENTRY_ENVIRONMENT"),
        )

    loop = asyncio.new_event_loop()
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
