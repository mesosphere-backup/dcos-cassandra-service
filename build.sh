#!/usr/bin/env bash

# In theory, we could use Jenkins' "Multi SCM" script, but good luck with getting auto-build to work with that
# Instead, clone the secondary 'dcos-tests' repo manually.
git clone --depth 1 git@github.com:mesosphere/dcos-tests.git
echo Running with dcos-tests rev: $(git --git-dir=dcos-tests/.git rev-parse HEAD)

# GitHub notifier config
export _GITHUB_SCRIPT="dcos-tests/build/update-github-status.sh"
export GITHUB_COMMIT_STATUS_URL="http://velocity.mesosphere.com/service/velocity/job/${JOB_NAME}/${BUILD_ID}/console"
export GIT_REPOSITORY_ROOT=$(pwd)

# Build steps for Cassandra

$_GITHUB_SCRIPT pending build "Build running"

./gradlew clean distZip
if [ $? -ne 0 ]; then
  $_GITHUB_SCRIPT failure build "Gradle build failed"
  exit 1
fi

# try disabling 'org.gradle.parallel', which seems to cause this step to hang:
sed -i 's/parallel=true/parallel=false/g' gradle.properties
./gradlew check
if [ $? -ne 0 ]; then
  $_GITHUB_SCRIPT failure build "Unit tests failed"
  exit 1
fi

# hack: cli tests want EXACTLY python 3.4, while alpine is on 3.5
ln -s /usr/bin/python3 /usr/bin/python3.4
which python3.4
make --directory=cli/ all
if [ $? -ne 0 ]; then
  $_GITHUB_SCRIPT error build "CLI build/tests failed"
  exit 1
fi

$_GITHUB_SCRIPT success build "Build succeeded"

# No more github updates from here onwards:
# ci-test.sh and ci-upload.sh helpers both handle this internally

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
