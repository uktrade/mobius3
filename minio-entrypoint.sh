#!/bin/sh

set -e

echo $MINIO_ACCESS_KEY
mkdir -p /test-data/my-bucket

mkdir -p /root/.minio/certs
openssl req -new -newkey rsa:2048 -days 3650 -nodes -x509 -subj /CN=selfsigned \
    -keyout /root/.minio/certs/private.key \
    -out /root/.minio/certs/public.crt

/usr/bin/docker-entrypoint.sh -- "$@"
