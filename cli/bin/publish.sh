#!/usr/bin/env bash

# env vars
#   PLATFORM (e.g. linux-x86-64)

set -eux

export VERSION="${GIT_BRANCH#refs/tags/}"
export S3_DIR_URL="s3://downloads.mesosphere.io/cassandra/assets/${VERSION}/cli/${PLATFORM}"

make clean binary
aws s3 cp dist/dcos-cassandra "${S3_DIR_URL}/dcos-cassandra"
aws s3 cp dist/dcos-datastax "${S3_DIR_URL}/dcos-datastax"
sha256sum dist/dcos-cassandra dist/dcos-datastax
