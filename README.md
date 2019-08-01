# mobius3 [![CircleCI](https://circleci.com/gh/uktrade/mobius3.svg?style=svg)](https://circleci.com/gh/uktrade/mobius3) [![Test Coverage](https://api.codeclimate.com/v1/badges/dbfbc5b6e383d54ee69a/test_coverage)](https://codeclimate.com/github/uktrade/mobius3/test_coverage)

Continuously and asynchronously sync a local folder to an S3 bucket. This is a Python application, suitable for situations where

- FUSE cannot be used, such as in AWS Fargate;
- high performance local access is more important than synchronous saving to S3;
- there can be frequent modifications to the same file monitored by a single client;
- there are infrequent concurrent modifications to the same file from different clients;
- local files can be changed by any program;
- there are at most ~10k files to sync;
- changes in the S3 bucket may be performed directly i.e. not using mobius3.

These properties make mobius3 similar to a Dropbox or Google Drive client. Under the hood, [inotify](http://man7.org/linux/man-pages/man7/inotify.7.html) is used and so only Linux is supported.

> Early version. Please consider enabling versioning on the S3 bucket to avoid data loss.


## Installation

```bash
pip install mobius3
```

## Usage

mobius3 can be used a standalone command-line application

```bash
mobius3 /local/folder https://remote-bucket.s3-eu-west-2.amazonaws.com/ eu-west-2 --prefix folder/
```

or from Docker

```bash
docker run --rm -it \
    -v /local/folder:/home/mobius3/data \
    -e AWS_ACCESS_KEY_ID \
    -e AWS_SECRET_ACCESS_KEY \
    quay.io/uktrade/mobius3:v0.0.8 \
    mobius3 \
        /home/mobius3/data \
        https://remote-bucket.s3-eu-west-2.amazonaws.com/ \
        eu-west-2 \
        --prefix my-prefix/
```

or from asyncio Python

```python
from mobius3 import Syncer

start, stop = Syncer('/local/folder', 'https://remote-bucket.s3-eu-west-2.amazonaws.com/', 'eu-west-2', prefix='folder/')

# Will copy the contents of the bucket to the local folder,
# raise exceptions on error, and then continue to sync in the background
await start()

# Will complete any remaining uploads
await stop()
```

In the cases above AWS credentials are taken from the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables. To use ECS-provided credentials / IAM Roles, you can pass `--credentials-source ecs-container-endpoint` as a command line option. In an ECS task definition, this would look something like the below

```json
{
	"command": [
		"mobius3",
		"/home/mobius3/data",
		"https://remote-bucket.s3-eu-west-2.amazonaws.com/",
		"eu-west-2",
		"--prefix", "my-prefix/"
		"--credentials-source", "ecs-container-endpoint"
	]
}
```

If using mobius3 to sync data in a volume accessed by multiple containers, you may have to create your own Dockerfile that runs mobius3 under a user with the same ID as the users in the other containers.


## Under the hood and limitations

Renaming files or folders map to no atomic operation in S3, and there is no explicit conflict resolution, so conflicts are resolved by S3 itself: the last write wins. This means that with concurrent modifications or deletions to the same file(s) or folder(s) by different clients, _data can be lost_ and the directory layout may get corrupted.

A simple polling mechanism is used to check for changes in S3: hence for large number of files/objects mobius3 may not be performant.

However, there is an exception to the above behaviour: if a file has been updated or deleted by a local process, until 120 seconds after the completion of its upload to S3, it will not be updated by a poll to S3. This is a best-effort attempt to mitigate the possibility of older versions overwriting newer due to the eventual consistency model of S3.

Uploads to S3 are initiated when a file is closed.

Some of the above behaviours may change in future versions.


### Concurrency: responding to concurrent file modifications

Mid-upload, a file can could modified by a local process, so in this case a corrupt file could be uploaded to S3. To mitigate this mobius3 uses the following algorithm for each upload.

- An IN_CLOSE_WRITE event is received for a file, and we start the upload.
- Just before the end of the upload, the final bytes of the file are read from disk.
- A dummy "flush" file is written to the relevant directory.
- Wait for the IN_CREATE event for this file. This ensures that any events since the final bytes were read have also been received.
- If we received an IN_MODIFY event for the file, the file has been modified, and we do not upload the final bytes. Since IN_MODIFY was received, once the file is closed we will receive an IN_CLOSE_WRITE, and we re-upload the file. If not such event is received, we complete the upload.

An alternative to the above would be use a filesystem locking mechanism. However

- other processes may not respect advisary locking;
- the filesystem may not support mandatory locking;
- we don't want to prevent other processes from progressing due to locking the file on upload: this would partially remove the benefits of the asynchronous nature of the syncing.


### Concurrency: keeping HTTP requests for the same file ordered

Multiple concurrent requests to S3 are also supported. However, this presents the possibility of additional race conditions: requests started in a given order may not be received by S3 in that order. This means that newer versions of files can be overwritten by older. Even the guarantee from S3 that "latest time stamp wins" for concurrent PUTs to the same key does not offer sufficient protection from this, since such requests can be made with the same timestamp.

Therefore to prevent this, a FIFO mutex is used around each file during PUT and DELETE of any key.


## Running tests

```bash
docker-compose build && \
docker-compose run --rm test python3 setup.py test
```
