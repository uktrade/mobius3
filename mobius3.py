import array
import argparse
import asyncio
from collections import (
    defaultdict,
)
import ctypes
import datetime
import enum
import fcntl
import termios
import json
import logging
import os
import re
import shutil
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
    HttpConnectionError,
    HttpDataError,
    HttpLoggerAdapter,
    Pool,
    buffered,
    empty_async_iterator,
    timeout,
)
from lowhaio_aws_sigv4_unsigned_payload import (
    aws_sigv4_headers,
)
from lowhaio_retry import (
    retry,
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

# We watch the download directly only for moves to be able to use the cookie
# to determine if a move is from a download so we then don't re-upload it
DOWNLOAD_WATCH_MASK = \
    InotifyEvents.IN_MOVED_FROM

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
            expiration = datetime.datetime.strptime(creds['Expiration'], '%Y-%m-%dT%H:%M:%SZ')
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
        flush_file_root='.__mobius3_flush__',
        flush_file_timeout=5,
        directory_watch_timeout=5,
        download_directory='.mobius3',
        get_logger_adapter=get_logger_adapter_default,
        get_http_logger_adapter=get_http_logger_adapter_default,
        get_resolver_logger_adapter=get_resolver_logger_adapter_default,
        local_modification_persistance=120,
        download_interval=10,
        exclude_remote=re.compile(r'^$'),
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

    # When downloading a file, we note its etag. We don't re-download it later
    # if the etag matches
    etags = {}

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

    retriable_request = retry(request, exception_intervals=(
        (HttpConnectionError, (0, 0, 0)),
        (HttpDataError, (0, 1, 2, 4, 8, 16)),
    ))
    signed_request = signed(
        retriable_request, credentials=get_credentials, service='s3', region=region,
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

    def queued_push_local_change(path):
        push_queued[path] += 1

    def completed_push_local_change(path):
        push_queued[path] -= 1
        if push_queued[path] == 0:
            del push_queued[path]
        push_completed.add(path)

    def is_pull_blocked(path):
        # For extremely recent modifications we may not have yielded the event
        # loop to add files to the queue. We do our best and check the mtime
        # to prevent overriding with older remote data. However, after we
        # check a file could still be modified locally, and we have no way to
        # detect this

        def modified_recently():
            now = datetime.datetime.now().timestamp()
            try:
                return now - os.path.getmtime(path) < local_modification_persistance
            except FileNotFoundError:
                return False

        return path in push_queued or path in push_completed or modified_recently()

    async def start():
        logger = get_logger_adapter({'mobius3_component': 'start'})
        logger.info('Starting')
        nonlocal upload_tasks
        nonlocal download_tasks
        nonlocal download_manager_task
        os.mkdir(directory / download_directory)
        upload_tasks = [
            asyncio.create_task(process_jobs(upload_job_queue))
            for i in range(0, concurrent_uploads)
        ]
        download_tasks = [
            asyncio.create_task(process_jobs(download_job_queue))
            for i in range(0, concurrent_downloads)
        ]
        start_inotify(logger)
        await list_and_schedule_downloads(logger)
        await download_job_queue.join()
        download_manager_task = asyncio.create_task(
            download_manager(get_logger_adapter({'mobius3_component': 'download'}))
        )
        logger.info('Finished starting')

    def start_inotify(logger):
        nonlocal wds_to_path
        nonlocal tree_cache_root
        nonlocal fd
        nonlocal ignore_next_delete
        ignore_next_delete = {}
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
        watch_directory(download_directory, DOWNLOAD_WATCH_MASK)
        watch_and_upload_directory(logger, directory, WATCH_MASK)

    async def stop():
        # Make every effort to read all incoming events and finish the queue
        logger = get_logger_adapter({'mobius3_component': 'stop'})
        logger.info('Stopping')
        download_manager_task.cancel()
        for task in download_tasks:
            task.cancel()
        read_events(logger)
        while upload_job_queue._unfinished_tasks:
            await upload_job_queue.join()
            read_events(logger)
        stop_inotify()
        for task in upload_tasks:
            task.cancel()
        await close_pool()
        await asyncio.sleep(0)
        shutil.rmtree(directory / download_directory)
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

    def watch_and_upload_directory(logger, path, mask):
        watch_directory(path, mask)

        # By the time we've added a watcher, files or subdirectories may have
        # already been created
        for root, dirs, files in os.walk(path):
            if PurePosixPath(root) == directory / download_directory:
                continue
            for file in files:
                logger.info('Scheduling upload: %s', PurePosixPath(root) / file)
                schedule_upload(logger, PurePosixPath(root) / file)

            for d in dirs:
                watch_and_upload_directory(logger, PurePosixPath(root) / d, mask)

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
        start_inotify(logger)

    def handle__flush__IN_CREATE(logger, _, __, path):
        flush = flushes[path]
        logger.debug('Flushing: %s', path)
        flush.set()

    def handle__download__IN_MOVED_FROM(logger, __, cookie, ___):
        logger.debug('Cookie: %s', cookie)
        download_cookies.add(cookie)

    def handle__file__IN_CLOSE_WRITE(logger, _, __, path):
        schedule_upload(logger, path)

    def handle__dir__IN_CREATE(logger, _, __, path):
        watch_and_upload_directory(logger, path, WATCH_MASK)

    def handle__file__IN_DELETE(logger, _, __, path):
        try:
            del etags[path]
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
        watch_and_upload_directory(logger, path, WATCH_MASK)

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

    def schedule_upload(logger, path):
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

    def schedule_delete(logger, path):
        version_current = get_content_version(path)
        version_original = version_current.copy()

        async def function():
            try:
                await delete(logger, path, version_current, version_original)
            finally:
                completed_push_local_change(path)

        try:
            remove_file_from_tree_cache(path)
        except KeyError:
            # Create events for files do not register a file in the cache,
            # until they are scheduled for upload on modification. If this
            # doesn't happen, then the file won't be in the cache, and we
            # have nothing to upload
            return
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

        content_length = str(os.stat(path).st_size).encode()
        mtime = str(os.path.getmtime(path)).encode()

        # Ensure we only progress if the content length hasn't changed since
        # we have queued the upload
        await flush_events(logger, path)
        if content_version_current != content_version_original:
            raise FileContentChanged(path)

        headers = await locked_request(
            logger, b'PUT', path, body=file_body,
            headers=(
                (b'content-length', content_length),
                (b'x-amz-meta-mtime', mtime),
            ),
        )
        etag = dict((key.lower(), value) for key, value in headers)[b'etag'].decode()
        etags[path] = etag

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

        return headers

    async def download_manager(logger):
        while True:
            try:
                await list_and_schedule_downloads(logger)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception('Failed to list files')
            await download_job_queue.join()
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

        full_paths = set(directory / path for path, _ in path_etags)
        for root, _, files in os.walk(directory):
            for file in files:
                full_path = PurePosixPath(root) / file
                if full_path in full_paths or is_pull_blocked(full_path):
                    continue

                # Since walking the filesystem can take time we might have a new file that we have
                # recently uploaded that was not present when we request the original file list.
                path = full_path.relative_to(directory)
                code, _, body = await signed_request(logger, b'HEAD', bucket + prefix + str(path))
                await buffered(body)
                if code != b'404':
                    continue

                # Check again if we have made modifications since the above request can take time
                try:
                    await flush_events(logger, full_path)
                except FileNotFoundError:
                    continue
                if is_pull_blocked(full_path):
                    continue

                try:
                    logger.info('Deleting locally %s', full_path)
                    os.remove(full_path)
                except FileNotFoundError:
                    pass
                else:
                    # The remove will queue a remote DELETE. However, the file already doesn't
                    # appear to exist in S3, so a) there is no need and b) may actually delete
                    # data either added by another client, or even from this one in the case
                    # of an extremely long eventual consistency issue where a PUT of a object
                    # that did not previously exist is still appearing
                    ignore_next_delete[full_path] = True

    def schedule_download(logger, path):
        async def download():
            full_path = directory / path

            if is_pull_blocked(full_path):
                logger.debug('Recently changed locally, not changing: %s', full_path)
                return

            logger.info('Downloading: %s', full_path)

            code, headers, body = await signed_request(logger, b'GET', bucket + prefix + path)
            if code != b'200':
                await buffered(body)  # Fetch all bytes and return to pool
                raise Exception(code)

            headers_dict = dict((key.lower(), value) for key, value in headers)
            parent_directory = directory / (PurePosixPath(path).parent)
            try:
                os.makedirs(parent_directory)
            except FileExistsError:
                logger.debug('Already exists: %s', parent_directory)
            except NotADirectoryError:
                logger.debug('Not a directory: %s', parent_directory)
            except Exception:
                logger.debug('Unable to create directory: %s', parent_directory)

            try:
                modified = float(headers_dict[b'x-amz-meta-mtime'])
            except (KeyError, ValueError):
                modified = datetime.datetime.strptime(
                    headers_dict[b'last-modified'].decode(),
                    '%a, %d %b %Y %H:%M:%S %Z').timestamp()

            temporary_path = directory / download_directory / uuid.uuid4().hex
            try:
                with open(temporary_path, 'wb') as file:
                    async for chunk in body:
                        file.write(chunk)

                # May raise a FileNotFoundError if the directory no longer
                # exists, but handled at higher level
                os.utime(temporary_path, (modified, modified))

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

                logger.debug('Moving to %s', full_path)
                os.replace(temporary_path, full_path)

                # Ensure that once we move the file into place, subsequent
                # renames will attempt to move the file
                ensure_file_in_tree_cache(full_path)
            finally:
                try:
                    os.remove(temporary_path)
                except FileNotFoundError:
                    pass
            etags[full_path] = headers_dict[b'etag'].decode()

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
                    if exclude_remote.match(key_relative):
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
    logger = logging.getLogger('mobius3')
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
