#!/bin/bash

# env vars
#   PLATFORM (e.g. linux-x86-64)

set -eux -o pipefail

export VERSION="${GIT_BRANCH#refs/tags/}"
export S3_URL="s3://downloads.mesosphere.io/cassandra/assets/${VERSION}/cli/${PLATFORM}/dcos-cassandra"

make clean binary
aws s3 cp dist/dcos-cassandra "${S3_URL}"
