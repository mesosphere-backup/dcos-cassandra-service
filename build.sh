#!/usr/bin/env bash

REPO_ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $REPO_ROOT_DIR

# In theory, we could use Jenkins' "Multi SCM" script, but good luck with getting auto-build to work with that
# Instead, clone the secondary 'dcos-tests' repo manually.
if [ -d dcos-tests ]; then
  cd dcos-tests/
  git fetch --depth 1 origin bincli
  git checkout bincli
  cd ..
else
  git clone --depth 1 --branch bincli git@github.com:mesosphere/dcos-tests.git
fi
echo Running with dcos-tests rev: $(git --git-dir=dcos-tests/.git rev-parse HEAD)

# GitHub notifier config

_notify_github() {
    # IF THIS FAILS FOR YOU, your dcos-tests is out of date!
    # do this: rm -rf kafka-private/dcos-tests/ then run build.sh again
    $REPO_ROOT_DIR/dcos-tests/build/github_update.py $1 $2 $3
}

# Build steps for Cassandra

_notify_github pending build "Build running"

# Scheduler/Executor (Java):

./gradlew clean distZip
if [ $? -ne 0 ]; then
  _notify_github failure build "Gradle build failed"
  exit 1
fi

# try disabling 'org.gradle.parallel', which seems to cause this step to hang:
sed -i 's/parallel=true/parallel=false/g' gradle.properties
./gradlew check
if [ $? -ne 0 ]; then
  _notify_github failure build "Unit tests failed"
  exit 1
fi

# CLI (Go):

cd cli && ./build-cli.sh
if [ $? -ne 0 ]; then
  _notify_github failure build "CLI build failed"
  exit 1
fi
cd $REPO_ROOT_DIR

_notify_github success build "Build succeeded"

# No more github updates from here onwards:
# ci-test.sh and ci-upload.sh helpers both handle this internally

./dcos-tests/build/ci-upload.sh \
  cassandra \
  universe/ \
  cassandra-scheduler/build/distributions/scheduler.zip \
  cassandra-executor/build/distributions/executor.zip \
  cli/dcos-cassandra/dcos-cassandra-darwin \
  cli/dcos-cassandra/dcos-cassandra-linux \
  cli/dcos-cassandra/dcos-cassandra.exe \
  cli/python/dist/*.whl
if [ $? -ne 0 ]; then
  exit 1
fi
