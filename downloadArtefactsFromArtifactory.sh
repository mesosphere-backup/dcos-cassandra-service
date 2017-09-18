#!/bin/bash

set -e


LOCAL_DOWNLOAD_PATH=$1
REPO_URL=$2
RELEASE_VERSION=$3

ARTIFACTS_PATH=dcos/frameworks/mds-cassandra/${RELEASE_VERSION}

VERSION_FILE_NAME=version.txt
cat $VERSION_FILE_NAME | awk -f readProperties.awk > tempEnv.sh
source tempEnv.sh
rm tempEnv.sh

export FRAMEWORK_VERSION=${RELEASE_VERSION}
export JRE_FILE_NAME=$jre_file_name  #ex : jre-8u121-linux-x64.tar.gz
export LIB_MESOS_FILE_NAME=$libmesos_file_name #ex : libmesos-bundle-1.9-argus-1.1.x-3.tar.gz

export CASSANDRA_VERSION=$apache_cassandra_version
export FRAMEWORK_PLUS_CASSANDRA_VERSION="${FRAMEWORK_VERSION}-${CASSANDRA_VERSION}" # ex : 21-3.0.10



wget "${REPO_URL}/${ARTIFACTS_PATH}/scheduler.zip" -P $LOCAL_DOWNLOAD_PATH
wget "${REPO_URL}/${ARTIFACTS_PATH}/executor.zip"  -P $LOCAL_DOWNLOAD_PATH
wget "${REPO_URL}/${ARTIFACTS_PATH}/dcos-cassandra-darwin"   -P $LOCAL_DOWNLOAD_PATH 
wget "${REPO_URL}/${ARTIFACTS_PATH}/dcos-cassandra-linux"   -P $LOCAL_DOWNLOAD_PATH
wget "${REPO_URL}/${ARTIFACTS_PATH}/dcos-cassandra.exe"   -P $LOCAL_DOWNLOAD_PATH
wget "${REPO_URL}/${ARTIFACTS_PATH}/${LIB_MESOS_FILE_NAME}"   -P $LOCAL_DOWNLOAD_PATH
wget "${REPO_URL}/${ARTIFACTS_PATH}/${JRE_FILE_NAME}"   -P $LOCAL_DOWNLOAD_PATH
wget "${REPO_URL}/${ARTIFACTS_PATH}/apache-cassandra-${FRAMEWORK_PLUS_CASSANDRA_VERSION}-bin-dcos.tar.gz"   -P $LOCAL_DOWNLOAD_PATH
wget "${REPO_URL}/${ARTIFACTS_PATH}/bin_wrapper-0.0.1-py2.py3-none-any.whl"   -P $LOCAL_DOWNLOAD_PATH


