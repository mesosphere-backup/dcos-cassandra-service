set -e

S3_BUCKET_NAME=$1
AWS_SECRET_ACCESS_KEY=$2
AWS_ACCESS_KEY_ID=$3
RELEASE_VERSION=$4

export S3_BUCKET=$S3_BUCKET_NAME
export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
export RELEASE_VERSION=$RELEASE_VERSION


setProperty(){
  awk -v pat="^$1=" -v value="$1=$2" '{ if ($0 ~ pat) print value; else print $0; }' $3 > $3.tmp
  mv $3.tmp $3
}

VERSION_FILE_NAME=version.txt
cat $VERSION_FILE_NAME | awk -f readProperties.awk > tempEnv.sh
source tempEnv.sh
rm tempEnv.sh

FRAMEWORK_VERSION=$RELEASE_VERSION

FOLDER_NUMBER=$universe_folder_number
NEW_FOLDER_NUMBER=$(expr $FOLDER_NUMBER + 1) # ex 
FOLDER_NUMBER=$NEW_FOLDER_NUMBER # ex :

setProperty "universe_folder_number" $FOLDER_NUMBER $VERSION_FILE_NAME # updating $VERSION_FILE_NAME i.e universe_folder_number in version.txt
 
export JRE_FILE_NAME=$jre_file_name  #ex : jre-8u121-linux-x64.tar.gz
export LIB_MESOS_FILE_NAME=$libmesos_file_name #ex : libmesos-bundle-1.9-argus-1.1.x-3.tar.gz
export CASSANDRA_VERSION=$apache_cassandra_version
export FRAMEWORK_PLUS_CASSANDRA_VERSION="${FRAMEWORK_VERSION}-${CASSANDRA_VERSION}" # ex : 1.0.0-3.0.10


#genrating artefacts
#below export RELEASE_VERSION is needed for ./generateArtefacts.sh 
export RELEASE_VERSION=${RELEASE_VERSION}
./generateArtefacts.sh

if [ $? -ne 0 ]; then
	echo "Generating artifacts for framework failed." 
	exit 1
fi

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


rm -rf universeRepo
git clone https://github.com/adobe-mds/universe universeRepo
cd universeRepo
git checkout version-3.x
rm  -rf dev/repo/packages/M/mds-cassandra/${FOLDER_NUMBER}
mkdir  -p dev/repo/packages/M/mds-cassandra/${FOLDER_NUMBER}
cp  ../tmp/stub-universe-mds-cassandra/repo/packages/M/mds-cassandra/0/*  dev/repo/packages/M/mds-cassandra/${FOLDER_NUMBER}/

#creating universe json

bash scripts/build.sh dev

./uploadUniverseToS3.sh ${RELEASE_VERSION} $S3_BUCKET $AWS_SECRET_ACCESS_KEY $AWS_ACCESS_KEY_ID




