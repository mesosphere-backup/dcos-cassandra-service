#!/bin/bash

set -e


RELEASE_VERSION=$1
MDSBOT_ARTIFACTORY_APIKEY=$2
MDSBOT_ARTIFACTORY_USERNAME=$3
REPO_URL=$4
AWS_SECRET_ACCESS_KEY=$5
AWS_ACCESS_KEY_ID=$6
S3_BUCKET=$7

export RELEASE_VERSION=$RELEASE_VERSION
export MDSBOT_ARTIFACTORY_APIKEY=$MDSBOT_ARTIFACTORY_APIKEY
export MDSBOT_ARTIFACTORY_USERNAME=$MDSBOT_ARTIFACTORY_USERNAME
export REPO_URL=$REPO_URL
export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
export S3_BUCKET=$S3_BUCKET

git checkout mds-${RELEASE_VERSION}


#conitnue from here
###### reading properties from version.txt and as setting env variables 
VERSION_FILE_NAME=version.txt
cat $VERSION_FILE_NAME | awk -f readProperties.awk > tempEnv.sh
source tempEnv.sh
rm tempEnv.sh

export FRAMEWORK_VERSION=$mds_version
export JRE_FILE_NAME=$jre_file_name  #ex : jre-8u121-linux-x64.tar.gz
export LIB_MESOS_FILE_NAME=$libmesos_file_name #ex : libmesos-bundle-1.9-argus-1.1.x-3.tar.gz

export CASSANDRA_VERSION=$apache_cassandra_version
export FRAMEWORK_PLUS_CASSANDRA_VERSION="${FRAMEWORK_VERSION}-${CASSANDRA_VERSION}" # ex : 21-3.0.10
#######

rm -rf tempDownloadedLoc
mkdir -p tempDownloadedLoc

## to check if devstable universe exists 
./downloadArtefactsFromArtifactory.sh  tempDownloadedLoc ${REPO_URL} ${RELEASE_VERSION}


#uploading artefacts to maven artifactory
./uploadArtifactsAndGenrateUniverseFiles.sh \
	tempDownloadedLoc/scheduler.zip \
	tempDownloadedLoc/executor.zip \
	tempDownloadedLoc/dcos-cassandra-darwin \
	tempDownloadedLoc/dcos-cassandra-linux \
	tempDownloadedLoc/dcos-cassandra.exe \
	tempDownloadedLoc/${LIB_MESOS_FILE_NAME} \
	tempDownloadedLoc/${JRE_FILE_NAME} \
	tempDownloadedLoc/apache-cassandra-${FRAMEWORK_PLUS_CASSANDRA_VERSION}-bin-dcos.tar.gz \
	tempDownloadedLoc/bin_wrapper-0.0.1-py2.py3-none-any.whl
	


#adding universe files
./addUniverseFilesToUniverseRepo.sh mds-${RELEASE_VERSION} stg tmp/stub-universe-mds-cassandra/repo/packages/M/mds-cassandra/0  ${FRAMEWORK_VERSION}

