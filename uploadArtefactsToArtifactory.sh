set -e

if [ -z "$REPO_URL" ]; then
	echo " REPO_URL environment variable not set, so setting default"
fi

if [ -z "$RELEASE_VERSION" ]; then
	echo " RELEASE_VERSION environment variable not set, so setting default"
fi

if [ -z "$MDSBOT_ARTIFACTORY_APIKEY" ]; then
	echo " MDSBOT_ARTIFACTORY_APIKEY environment variable not set, so setting default"
fi

if [ -z "$MDSBOT_ARTIFACTORY_USERNAME" ]; then
	echo " MDSBOT_ARTIFACTORY_USERNAME environment variable not set, so setting default"
fi



ARTIFACTS_PATH=dcos/frameworks/mds-cassandra/${RELEASE_VERSION}


VERSION_FILE_NAME=version.txt
cat $VERSION_FILE_NAME | awk -f readProperties.awk > tempEnv.sh
source tempEnv.sh
rm tempEnv.sh

export FRAMEWORK_VERSION=$mds_version
export JRE_FILE_NAME=$jre_file_name  #ex : jre-8u121-linux-x64.tar.gz
export LIB_MESOS_FILE_NAME=$libmesos_file_name #ex : libmesos-bundle-1.9-argus-1.1.x-3.tar.gz

export CASSANDRA_VERSION=$apache_cassandra_version
export FRAMEWORK_PLUS_CASSANDRA_VERSION="${FRAMEWORK_VERSION}-${CASSANDRA_VERSION}" # ex : 21-3.0.10


curl -u $MDSBOT_ARTIFACTORY_USERNAME:$MDSBOT_ARTIFACTORY_APIKEY -X PUT "${REPO_URL}/${ARTIFACTS_PATH}/scheduler.zip" -T $1
curl -u $MDSBOT_ARTIFACTORY_USERNAME:$MDSBOT_ARTIFACTORY_APIKEY -X PUT "${REPO_URL}/${ARTIFACTS_PATH}/executor.zip" -T $2
curl -u $MDSBOT_ARTIFACTORY_USERNAME:$MDSBOT_ARTIFACTORY_APIKEY -X PUT "${REPO_URL}/${ARTIFACTS_PATH}/dcos-cassandra-darwin" -T $3
curl -u $MDSBOT_ARTIFACTORY_USERNAME:$MDSBOT_ARTIFACTORY_APIKEY -X PUT "${REPO_URL}/${ARTIFACTS_PATH}/dcos-cassandra-linux" -T $4
curl -u $MDSBOT_ARTIFACTORY_USERNAME:$MDSBOT_ARTIFACTORY_APIKEY -X PUT "${REPO_URL}/${ARTIFACTS_PATH}/dcos-cassandra.exe" -T $5
curl -u $MDSBOT_ARTIFACTORY_USERNAME:$MDSBOT_ARTIFACTORY_APIKEY -X PUT "${REPO_URL}/${ARTIFACTS_PATH}/${LIB_MESOS_FILE_NAME}" -T $6
curl -u $MDSBOT_ARTIFACTORY_USERNAME:$MDSBOT_ARTIFACTORY_APIKEY -X PUT "${REPO_URL}/${ARTIFACTS_PATH}/${JRE_FILE_NAME}" -T $7
curl -u $MDSBOT_ARTIFACTORY_USERNAME:$MDSBOT_ARTIFACTORY_APIKEY -X PUT "${REPO_URL}/${ARTIFACTS_PATH}/apache-cassandra-${FRAMEWORK_PLUS_CASSANDRA_VERSION}-bin-dcos.tar.gz" -T $8
curl -u $MDSBOT_ARTIFACTORY_USERNAME:$MDSBOT_ARTIFACTORY_APIKEY -X PUT "${REPO_URL}/${ARTIFACTS_PATH}/bin_wrapper-0.0.1-py2.py3-none-any.whl" -T $9






