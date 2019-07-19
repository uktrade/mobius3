# mobius3

Continuously and asynchronously sync a local folder to an S3 bucket. This is a Python application, suitable for situations where

- FUSE cannot be used, such as in AWS Fargate;
- high performance local access is more important than synchronous saving to S3;
- there are infrequent concurrent modifications to the same file from different clients;
- local files can be changed by any program;
- changes in the S3 bucket may be performed directly i.e. not using mobius3.

These properties make mobius3 similar to a Dropbox or Google Drive client. Under the hood, [inotify](http://man7.org/linux/man-pages/man7/inotify.7.html) is used and so only Linux is supported.

> Work in progress. This README is a rough design spec.


## Usage

```bash
mobius3 /local/folder https://s3-eu-west-2.amazonaws.com/remote-bucket/path-in-bucket
```
