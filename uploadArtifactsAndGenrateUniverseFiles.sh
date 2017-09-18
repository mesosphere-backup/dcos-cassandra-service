#!/bin/bash

set -e

##


# Upload artifacts and generate universe files
#

if [ "$#" -ne 9 ]; then
    echo "Illegal number of parameters"
fi

if [ -z "$S3_BUCKET" ]; then
    
	echo " S3_BUCKET environment variable not set, exiting"
	exit 1
fi

if [ -z "$RELEASE_VERSION" ]; then
    
	echo " RELEASE_VERSION environment variable not set, exiting"
	exit 1
fi

BUCKET=$S3_BUCKET

#used in build.sh to upload framework resources to s3
FOLDER_PATH=dcos/frameworks/mds-cassandra/${RELEASE_VERSION}
export S3_URL="s3://${BUCKET}/${FOLDER_PATH}"
#used in build.sh to upload framework resources to s3
export ARTIFACT_DIR="https://${BUCKET}.s3.amazonaws.com/${FOLDER_PATH}"


#####################################


VERSION_FILE_NAME=version.txt
TMPDIR_NAME=tmp
rm -rf $TMPDIR_NAME
mkdir $TMPDIR_NAME

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export SCRIPT_DIR=$SCRIPT_DIR

export TMPDIR=$SCRIPT_DIR/$TMPDIR_NAME
cat $VERSION_FILE_NAME | awk -f readProperties.awk > tempEnv.sh
source tempEnv.sh
rm tempEnv.sh


export CASSANDRA_VERSION=$apache_cassandra_version
export CASSANDRA_FILE_NAME="apache-cassandra-${apache_cassandra_version}-bin.tar.gz" #CASSANDRA_FILE_NAME=apache-cassandra-3.0.10-bin.tar.gz
export FRAMEWORK_VERSION=$RELEASE_VERSION
export JRE_FILE_NAME=$jre_file_name  #ex : jre-8u121-linux-x64.tar.gz
export JRE_EXTRACTED_FILE_NAME=$jre_extracted_file_name # ex : jre1.8.0_121
export JRE_VERSION=$jre_version  # ex : 8.0_121
export LIB_MESOS_FILE_NAME=$libmesos_file_name #ex : libmesos-bundle-1.9-argus-1.1.x-3.tar.gz

export FRAMEWORK_PLUS_CASSANDRA_VERSION="${FRAMEWORK_VERSION}-${CASSANDRA_VERSION}" # ex : 21-3.0.10
echo $FRAMEWORK_PLUS_CASSANDRA_VERSION

export TEMPLATE_jre_file_name=$JRE_FILE_NAME
export TEMPLATE_cassandra_version=$FRAMEWORK_PLUS_CASSANDRA_VERSION
export TEMPLATE_lib_mesos_file_name=$LIB_MESOS_FILE_NAME
export TEMPLATE_jre_extracted_file_name=$JRE_EXTRACTED_FILE_NAME

######################
#below env var are used in universe_builder ,while replacing template variables


export TEMPLATE_jre_file_name=$JRE_FILE_NAME
export TEMPLATE_cassandra_version=$FRAMEWORK_PLUS_CASSANDRA_VERSION
export TEMPLATE_lib_mesos_file_name=$LIB_MESOS_FILE_NAME
export TEMPLATE_jre_extracted_file_name=$JRE_EXTRACTED_FILE_NAME


./dcos-commons-tools/publish_aws.py \
	mds-cassandra \
	universe/ \
	$1 \
	$2 \
	$3 \
	$4 \
	$5 \
	$6 \
	$7 \
	$8 \
	$9
