#!/bin/bash

git checkout origin/master
git checkout -b master

DEFAULT_BUCKET=mds-dcos
DEFAULT_REGION=us-east-1

setProperty(){
  awk -v pat="^$1=" -v value="$1=$2" '{ if ($0 ~ pat) print value; else print $0; }' $3 > $3.tmp
  mv $3.tmp $3
}
#check if S3_BUCKET environment variable is set or not
if [ -z "$S3_BUCKET" ]; then
    export S3_BUCKET=$DEFAULT_BUCKET
	echo " S3_BUCKET environment variable not set, so setting default : ${DEFAULT_BUCKET}"
fi

#check if AWS_ACCESS_KEY_ID environment variable is set or not
if [ -z "$AWS_ACCESS_KEY_ID" ]; then
    echo "Need to set AWS_ACCESS_KEY_ID environment variable"
    exit 1
fi

#check if AWS_SECRET_ACCESS_KEY environment variable is set or not
if [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
    echo "Need to set AWS_SECRET_ACCESS_KEY environment variable"
    exit 1
fi

#check if GRADLE_USER_HOME environment variable is set or not
if [ -z "$GRADLE_USER_HOME" ]; then
    echo "Need to set GRADLE_USER_HOME environment variable"
    exit 1
fi


#check if MDS_DCOS_URL environment variable is set or not
if [ -z "$MDS_DCOS_URL" ]; then
    echo "Need to set MDS_DCOS_URL environment variable"
    exit 1
fi

#check if MDS_DCOS_TOKEN environment variable is set or not
if [ -z "$MDS_DCOS_TOKEN" ]; then
    echo "Need to set MDS_DCOS_TOKEN environment variable"
    exit 1
fi



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

#apache_cassandra_version=3.0.10
#version=20


export CASSANDRA_VERSION=$apache_cassandra_version
export CASSANDRA_FILE_NAME="apache-cassandra-${apache_cassandra_version}-bin.tar.gz" #CASSANDRA_FILE_NAME=apache-cassandra-3.0.10-bin.tar.gz
export FRAMEWORK_VERSION=$mds_version
export JRE_FILE_NAME=$jre_file_name  #ex : jre-8u121-linux-x64.tar.gz
export JRE_EXTRACTED_FILE_NAME=$jre_extracted_file_name # ex : jre1.8.0_121
export JRE_VERSION=$jre_version  # ex : 8.0_121
export LIB_MESOS_FILE_NAME=$libmesos_file_name #ex : libmesos-bundle-1.9-argus-1.1.x-3.tar.gz


NEW_FRAMEWORK_VERSION=$(expr $FRAMEWORK_VERSION + 1) # ex : version=21
export FRAMEWORK_VERSION=$NEW_FRAMEWORK_VERSION # ex : FRAMEWORK_VERSION now equal to 21

export FRAMEWORK_PLUS_CASSANDRA_VERSION="${FRAMEWORK_VERSION}-${CASSANDRA_VERSION}" # ex : 21-3.0.10
echo $FRAMEWORK_PLUS_CASSANDRA_VERSION
setProperty "mds_version" $FRAMEWORK_VERSION $VERSION_FILE_NAME # updating $VERSION_FILE_NAME i.e mds_version in version.txt

export TEMPLATE_jre_file_name=$JRE_FILE_NAME
export TEMPLATE_cassandra_version=$FRAMEWORK_PLUS_CASSANDRA_VERSION
export TEMPLATE_lib_mesos_file_name=$LIB_MESOS_FILE_NAME
export TEMPLATE_jre_extracted_file_name=$JRE_EXTRACTED_FILE_NAME

export JRE_DWNLD_URL=$jre_dwnld_url  #https://artifactory.corp.adobe.com/artifactory/maven-multicloud-release-local/dcos/cassandra/artifacts/jre-8u121-linux-x64.tar.gz
export APACHE_CASSANDRA_DWNLD_URL=$apache_cassandra_dwnld_url #https://artifactory.corp.adobe.com/artifactory/maven-multicloud-release-local/dcos/cassandra/artifacts/apache-cassandra-3.0.10-bin.tar.gz
export APACHE_CASSANDRA_SHA1_DWNLD_URL=$apache_cassandra_sha1_dwnld_url  # https://artifactory.corp.adobe.com/artifactory/maven-multicloud-release-local/dcos/cassandra/artifacts/apache-cassandra-3.0.10-bin.tar.gz.sha1
export LIB_MESOS_DWNLD_URL=$libmesos_dwnld_url #https://artifactory.corp.adobe.com/artifactory/maven-multicloud-release-local/dcos/cassandra/artifacts/libmesos-bundle-1.9-argus-1.1.x-3.tar.gz



BUCKET=$S3_BUCKET
REGION=$AWS_UPLOAD_REGION


#used in build.sh to upload framework resources to s3
FOLDER_PATH=dcos-frameworks/mds-cassandra/${FRAMEWORK_VERSION}
export S3_URL="s3://${BUCKET}/${FOLDER_PATH}"
#used in build.sh to upload framework resources to s3
export ARTIFACT_DIR="https://${BUCKET}.s3.amazonaws.com/${FOLDER_PATH}"

CHECK_FILE_PATH=${FOLDER_PATH}/changes.txt





#AWS S3 bucket checks
checkS3Bucket(){
	if [ -z "$AWS_UPLOAD_REGION" ]; then
	    echo "Aws S3 bucket region not set, so using default : $DEFAULT_REGION"
	    REGION=$DEFAULT_REGION
	fi

	if [ -z "$S3_BUCKET" ]; then
	    echo "Aws S3 bucket region not set, so using default : $DEFAULT_BUCKET"
	    BUCKET=$DEFAULT_BUCKET
	fi
	
	
	if aws s3 ls "s3://${BUCKET}" 2>&1 | grep -q 'InvalidAccessKeyId'
		then
			echo "Invalid Access Key Id"
			exit 1
	fi
	
	if aws s3 ls "s3://${BUCKET}" 2>&1 | grep -q 'SignatureDoesNotMatch'
		then
			echo "Invalid access key.Signature Does Not Match"
			exit 1
	fi
	
	if aws s3 ls "s3://${BUCKET}" 2>&1 | grep -q 'NoSuchBucket'
		then
			echo "Bucket doesn't exists,so creating new"
			aws s3api create-bucket --bucket $BUCKET --region $REGION
	fi

	count=`aws s3 ls s3://${BUCKET}/${CHECK_FILE_PATH} | wc -l`
	if [[ $count -gt 0 ]]; then
		echo "Folder with version ${FRAMEWORK_VERSION} already exists. Some problem , please check."
		exit 1
	else
		echo "Folder with ${FRAMEWORK_VERSION} doesn't exists"
	fi
	
echo "Bucket checks completed successfully"
}

#check if everthing required for s3 is there
checkS3Bucket

#git checkout -b version-$FRAMEWORK_VERSION && git add version.txt && git commit -m "Updated version to-${FRAMEWORK_VERSION}"

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


git add version.txt && git commit -m "dcos cassandra framework version updated to ${NEW_FRAMEWORK_VERSION}" && git push origin master
git checkout -b mds-${NEW_FRAMEWORK_VERSION} && git push origin mds-${NEW_FRAMEWORK_VERSION} 


# universe version update
#
#

rm -rf universeRepo
mkdir universeRepo
cd universeRepo
git clone git@github.com:adobe-mds/universe.git
cd universe 
mkdir -p repo/packages/M/mds-cassandra/${FRAMEWORK_VERSION}


cp ../../tmp/stub-universe-mds-cassandra/repo/packages/M/mds-cassandra/0/* repo/packages/M/mds-cassandra/${FRAMEWORK_VERSION}/

UNIVERSE_VERSION_FILE_NAME=version.txt

cat $UNIVERSE_VERSION_FILE_NAME | awk -f readProperties.awk > tempEnv.sh
source tempEnv.sh
rm tempEnv.sh

# ex : universe_version=20

UNIVERSE_VERSION=$universe_version
echo "universe version${UNIVERSE_VERSION}"
NEW_UNIVERSE_VERSION=$(expr $UNIVERSE_VERSION + 1) # ex : universe_version=21

setProperty "universe_version" $NEW_UNIVERSE_VERSION $UNIVERSE_VERSION_FILE_NAME # updating $VERSION_FILE_NAME i.e universe_version in version.txt

git status && git add . && git commit -m "Added cassandra universe files for version : ${FRAMEWORK_VERSION}" && git push origin version-3.x

git checkout -b mds-${NEW_UNIVERSE_VERSION}
git push origin mds-${NEW_UNIVERSE_VERSION}
export UNIVERSE_VERSION=$NEW_UNIVERSE_VERSION
echo "universe version${UNIVERSE_VERSION}"
./generateUniverseAndUploadToS3.sh





