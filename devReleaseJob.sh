#!/bin/bash

set -e


RELEASE_VERSION=$1
MDSBOT_ARTIFACTORY_APIKEY=$2
MDSBOT_ARTIFACTORY_USERNAME=$3
REPO_URL=$4
AWS_SECRET_ACCESS_KEY=$5
AWS_ACCESS_KEY_ID=$6
S3_BUCKET=$7

ENVIRONMENT=dev

export RELEASE_VERSION=$RELEASE_VERSION
export MDSBOT_ARTIFACTORY_APIKEY=$MDSBOT_ARTIFACTORY_APIKEY
export MDSBOT_ARTIFACTORY_USERNAME=$MDSBOT_ARTIFACTORY_USERNAME
export REPO_URL=$REPO_URL
export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
export S3_BUCKET=$S3_BUCKET

git checkout mds-${RELEASE_VERSION}

if [ $? -ne 0 ]; then
	echo "Failed : Increment framework version and cut branch." 
	exit 1
fi


#genrating artefacts
bash generateArtefacts.sh

if [ $? -ne 0 ]; then
	echo "Generating artifacts for framework failed." 
	exit 1
fi


###### reading properties from version.txt and as setting env variables 
VERSION_FILE_NAME=version.txt
cat $VERSION_FILE_NAME | awk -f readProperties.awk > tempEnv.sh
source tempEnv.sh
rm tempEnv.sh

export FRAMEWORK_VERSION=$RELEASE_VERSION
UNIVERSE_FOLDER_NUMBER=$universe_folder_number
export JRE_FILE_NAME=$jre_file_name  #ex : jre-8u121-linux-x64.tar.gz
export LIB_MESOS_FILE_NAME=$libmesos_file_name #ex : libmesos-bundle-1.9-argus-1.1.x-3.tar.gz

export CASSANDRA_VERSION=$apache_cassandra_version
export FRAMEWORK_PLUS_CASSANDRA_VERSION="${FRAMEWORK_VERSION}-${CASSANDRA_VERSION}" # ex : 1.0.0-3.0.10
#######

#uploading artefacts to maven artifactory
bash uploadArtefactsToArtifactory.sh \
	cassandra-scheduler/build/distributions/scheduler.zip \
	cassandra-executor/build/distributions/executor.zip \
	cli/dcos-cassandra/dcos-cassandra-darwin \
	cli/dcos-cassandra/dcos-cassandra-linux \
	cli/dcos-cassandra/dcos-cassandra.exe \
	downloaded/artifacts/${LIB_MESOS_FILE_NAME} \
	downloaded/artifacts/${JRE_FILE_NAME} \
	cassandra-bin-tmp/apache-cassandra-${FRAMEWORK_PLUS_CASSANDRA_VERSION}-bin-dcos.tar.gz \
	cli/python/dist/bin_wrapper-0.0.1-py2.py3-none-any.whl


if [ $? -ne 0 ]; then
	echo "cassandra framework : Uploading artefacts to artifactory failed" 
	exit 1
fi

#uploading artefacts to maven artifactory
./uploadArtifactsAndGenrateUniverseFiles.sh \
	cassandra-scheduler/build/distributions/scheduler.zip \
	cassandra-executor/build/distributions/executor.zip \
	cli/dcos-cassandra/dcos-cassandra-darwin \
	cli/dcos-cassandra/dcos-cassandra-linux \
	cli/dcos-cassandra/dcos-cassandra.exe \
	downloaded/artifacts/${LIB_MESOS_FILE_NAME} \
	downloaded/artifacts/${JRE_FILE_NAME} \
	cassandra-bin-tmp/apache-cassandra-${FRAMEWORK_PLUS_CASSANDRA_VERSION}-bin-dcos.tar.gz \
	cli/python/dist/bin_wrapper-0.0.1-py2.py3-none-any.whl
	
if [ $? -ne 0 ]; then
	echo "cassandra framework : Uploading artefacts to s3 and universe files generation failed" 
	exit 1
fi


#adding universe files to repo.
bash addUniverseFilesToUniverseMainBranch.sh  $ENVIRONMENT tmp/stub-universe-mds-cassandra/repo/packages/M/mds-cassandra/0  ${FRAMEWORK_VERSION} ${UNIVERSE_FOLDER_NUMBER}
if [ $? -ne 0 ]; then
	echo "cassandra framework : adding universe files to  universe repo failed" 
	exit 1
fi






