#!/usr/bin/env bash

# In theory, we could use Jenkins' "Multi SCM" script, but good luck with getting auto-build to work with that
# Instead, clone the secondary 'dcos-tests' repo manually.
if [ ! -d dcos-tests ]; then
    git clone --depth 1 git@github.com:mesosphere/dcos-tests.git
fi
echo Running with dcos-tests rev: $(git --git-dir=dcos-tests/.git rev-parse HEAD)

# GitHub notifier config

if [ -n "$JENKINS_HOME" ]; then
    # we're in a CI build. send outcomes to github.
    # note: we expect upstream to specify GITHUB_COMMIT_STATUS_URL and GIT_REPOSITORY_ROOT in this case
    _notify_github() {
        ./dcos-tests/build/update-github-status.sh $1 $2 $3
    }
else
    # we're being run manually. print outcomes.
    _notify_github() {
        echo "[STATUS:build.sh] $2 $1: $3"
    }
fi

# Build steps for Cassandra

_notify_github pending build "Build running"

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

make --directory=cli/ all
if [ $? -ne 0 ]; then
  _notify_github error build "CLI build/tests failed"
  exit 1
fi

_notify_github success build "Build succeeded"

# No more github updates from here onwards:
# ci-test.sh and ci-upload.sh helpers both handle this internally

# DataStax Cassandra
./dcos-tests/build/ci-upload.sh \
  datastax \
  universe-datastax/index.json \
  universe-datastax/package/ \
  cassandra-scheduler/build/distributions/scheduler.zip \
  cassandra-executor/build/distributions/executor.zip \
  cli/pkg-datastax/dist/dcos-datastax-0.1.0.tar.gz
if [ $? -ne 0 ]; then
  exit 1
fi

# Apache Cassandra (used for any tests that follow)
./dcos-tests/build/ci-upload.sh \
  cassandra \
  universe/index.json \
  universe/package/ \
  cassandra-scheduler/build/distributions/scheduler.zip \
  cassandra-executor/build/distributions/executor.zip \
  cli/dist/dcos-cassandra-0.1.0.tar.gz
if [ $? -ne 0 ]; then
  exit 1
fi
