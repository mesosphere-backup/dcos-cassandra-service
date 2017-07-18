#!/bin/bash

set -e

VERSION_FILE_NAME=version.txt
TMPDIR_NAME=tmp
rm -rf $TMPDIR_NAME
mkdir $TMPDIR_NAME

rm -rf cassandra-bin-tmp
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export SCRIPT_DIR=$SCRIPT_DIR

export TMPDIR=$SCRIPT_DIR/$TMPDIR_NAME
cat $VERSION_FILE_NAME | awk -f readProperties.awk > tempEnv.sh
source tempEnv.sh
rm tempEnv.sh


export CASSANDRA_VERSION=$apache_cassandra_version
export CASSANDRA_FILE_NAME="apache-cassandra-${apache_cassandra_version}-bin.tar.gz" #CASSANDRA_FILE_NAME=apache-cassandra-3.0.10-bin.tar.gz
export FRAMEWORK_VERSION=$mds_version

echo "$FRAMEWORK_VERSION"

export JRE_FILE_NAME=$jre_file_name  #ex : jre-8u121-linux-x64.tar.gz
export JRE_EXTRACTED_FILE_NAME=$jre_extracted_file_name # ex : jre1.8.0_121
export JRE_VERSION=$jre_version  # ex : 8.0_121
export LIB_MESOS_FILE_NAME=$libmesos_file_name #ex : libmesos-bundle-1.9-argus-1.1.x-3.tar.gz

export FRAMEWORK_PLUS_CASSANDRA_VERSION="${FRAMEWORK_VERSION}-${CASSANDRA_VERSION}" # ex : 21-3.0.10

export TEMPLATE_jre_file_name=$JRE_FILE_NAME
export TEMPLATE_cassandra_version=$FRAMEWORK_PLUS_CASSANDRA_VERSION
export TEMPLATE_lib_mesos_file_name=$LIB_MESOS_FILE_NAME
export TEMPLATE_jre_extracted_file_name=$JRE_EXTRACTED_FILE_NAME

export JRE_DWNLD_URL=$jre_dwnld_url  #https://artifactory.corp.adobe.com/artifactory/maven-multicloud-release-local/dcos/cassandra/artifacts/jre-8u121-linux-x64.tar.gz
export APACHE_CASSANDRA_DWNLD_URL=$apache_cassandra_dwnld_url #https://artifactory.corp.adobe.com/artifactory/maven-multicloud-release-local/dcos/cassandra/artifacts/apache-cassandra-3.0.10-bin.tar.gz
export APACHE_CASSANDRA_SHA1_DWNLD_URL=$apache_cassandra_sha1_dwnld_url  # https://artifactory.corp.adobe.com/artifactory/maven-multicloud-release-local/dcos/cassandra/artifacts/apache-cassandra-3.0.10-bin.tar.gz.sha1
export LIB_MESOS_DWNLD_URL=$libmesos_dwnld_url #https://artifactory.corp.adobe.com/artifactory/maven-multicloud-release-local/dcos/cassandra/artifacts/libmesos-bundle-1.9-argus-1.1.x-3.tar.gz

mkdir -p gopath
export GOPATH=${SCRIPT_DIR}/gopath

bash build-cassandra-bin.bash
if [ $? -ne 0 ]; then
	echo 'Error -----> creating customized Cassandra tar failed.'
  exit 1
fi

# sed -i "s/{cassVer}/${CASSANDRA_VERSION}/g" cassandra-scheduler/build.gradle
# sed -i "s/{cassVer}/${CASSANDRA_VERSION}/g" cassandra-commons/build.gradle

bash build.sh

if [ $? -ne 0 ]; then
	echo 'Error -----> Some error in creating framework artifacts.'
  exit 1
fi

