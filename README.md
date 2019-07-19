# mobius3 [![CircleCI](https://circleci.com/gh/uktrade/mobius3.svg?style=svg)](https://circleci.com/gh/uktrade/mobius3)

Continuously and asynchronously sync a local folder to an S3 bucket. This is a Python application, suitable for situations where

- FUSE cannot be used, such as in AWS Fargate;
- high performance local access is more important than synchronous saving to S3;
- there are infrequent concurrent modifications to the same file from different clients;
- local files can be changed by any program;
- there are at most ~10k files to sync;
- changes in the S3 bucket may be performed directly i.e. not using mobius3.

These properties make mobius3 similar to a Dropbox or Google Drive client. Under the hood, [inotify](http://man7.org/linux/man-pages/man7/inotify.7.html) is used and so only Linux is supported.

> Work in progress. This README is a rough design spec.


## Usage

mobius3 can be used a standalone command-line application

```bash
mobius3 /local/folder https://remote-bucket.s3-eu-west-2.amazonaws.com/path/in/bucket eu-west-2
```

or from asyncio Python

```python
from mobius3 import Syncer

start, stop = Syncer('/local/folder', 'https://remote-bucket.s3-eu-west-2.amazonaws.com/path/in/bucket', 'eu-west-2')

# Will copy the contents of the bucket to the local folder,
# raise exceptions on error, and then continue to sync in the background
await start()

# Will stop syncing
await stop()
```

AWS Credentials are taken from the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables.


## Under the hood and limitations

Renaming files or folders map to no atomic operation in S3, and conflicts are dealt with where S3 is always the source-of-truth. This means that with concurrent modifications or deletions to the same file(s) or folder(s) _data can be lost_ and the directory layout may get corrupted.

A simple polling mechanism is used to check for changes in S3: hence for large number of files/objects mobius3 may not be performant.

However, every effort is made so that content of each file is not corrupted, i.e. files mid-way through being changed locally are _not_ uploaded until they stop being changed.

Some of the above behaviours may change in future versions.


## Running tests

```bash
docker-compose build && \
docker-compose run --rm test python3 setup.py test
```
