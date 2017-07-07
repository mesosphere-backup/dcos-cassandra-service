#!/usr/bin/env bash

# Prevent jenkins from immediately killing the script when a step fails, allowing us to notify github:
set +e

export REPO_ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $REPO_ROOT_DIR

# Grab dcos-commons build/release tools:
#rm -rf dcos-commons-tools/ && curl https://infinity-artifacts.s3.amazonaws.com/dcos-commons-tools.tgz | tar xz

# GitHub notifier config
_notify_github() {
   #$REPO_ROOT_DIR/dcos-commons-tools/github_update.py $1 build $2
   echo $2
}

# Build steps for Cassandra

_notify_github pending "Build running"

# Scheduler/Executor (Java):

./gradlew --refresh-dependencies distZip
if [ $? -ne 0 ]; then
  _notify_github failure "Gradle build failed"
  exit 1
fi

# try disabling 'org.gradle.parallel', which seems to cause this step to hang:
sed -i 's/parallel=true/parallel=false/g' gradle.properties
./gradlew check
if [ $? -ne 0 ]; then
  _notify_github failure "Unit tests failed"
  exit 1
fi

# CLI (Go):

cd cli/ && ./build-cli.sh
if [ $? -ne 0 ]; then
  _notify_github failure "CLI build failed"
 # exit 1
fi
cd $REPO_ROOT_DIR

_notify_github success "Build succeeded"

mkdir -p downloaded/artifacts/

wget  ${LIB_MESOS_DWNLD_URL} -P downloaded/artifacts/
wget  ${JRE_DWNLD_URL} -P downloaded/artifacts/



./dcos-commons-tools/publish_aws.py \
 mds-cassandra \
  universe/ \
  cassandra-scheduler/build/distributions/scheduler.zip \
  cassandra-executor/build/distributions/executor.zip \
  cli/dcos-cassandra/dcos-cassandra-darwin \
  cli/dcos-cassandra/dcos-cassandra-linux \
  cli/dcos-cassandra/dcos-cassandra.exe \
	downloaded/artifacts/${LIB_MESOS_FILE_NAME} \
	downloaded/artifacts/${JRE_FILE_NAME} \
	resources/changes.txt \
	cassandra-bin-tmp/apache-cassandra-${FRAMEWORK_PLUS_CASSANDRA_VERSION}-bin-dcos.tar.gz \
  cli/python/dist/*.whl
