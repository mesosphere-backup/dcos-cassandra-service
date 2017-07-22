set -e

if [ -z "$RELEASE_VERSION" ]; then
	echo " RELEASE_VERSION environment variable not set, so setting default"
fi

VERSION_FILE_NAME=version.txt
cat $VERSION_FILE_NAME | awk -f readProperties.awk > tempEnv.sh
source tempEnv.sh
rm tempEnv.sh

export FRAMEWORK_VERSION=$RELEASE_VERSION
export JRE_FILE_NAME=$jre_file_name  #ex : jre-8u121-linux-x64.tar.gz
export LIB_MESOS_FILE_NAME=$libmesos_file_name #ex : libmesos-bundle-1.9-argus-1.1.x-3.tar.gz

export CASSANDRA_VERSION=$apache_cassandra_version
export FRAMEWORK_PLUS_CASSANDRA_VERSION="${FRAMEWORK_VERSION}-${CASSANDRA_VERSION}" # ex : 1.0.0-3.0.10

LOCAL_PATH_TO_CHECK=$1

if [ ! -f "$LOCAL_PATH_TO_CHECK/scheduler.zip" ]
	then
		echo "scheduler.zip file does not exist. Exiting..."
		exit 1
fi


if [ ! -f "$LOCAL_PATH_TO_CHECK/executor.zip" ]
	then
		echo "executor.zip file does not exist. Exiting..."
		exit 1
fi

if [ ! -f "$LOCAL_PATH_TO_CHECK/dcos-cassandra-darwin" ]
	then
		echo "dcos-cassandra-darwin file does not exist. Exiting..."
		exit 1
fi

if [ ! -f "$LOCAL_PATH_TO_CHECK/dcos-cassandra-linux" ]
	then
		echo "dcos-cassandra-linux file does not exist. Exiting..."
		exit 1
fi

if [ ! -f "$LOCAL_PATH_TO_CHECK/dcos-cassandra.exe" ]
	then
		echo "dcos-cassandra.exe file does not exist. Exiting..."
		exit 1
fi

if [ ! -f "$LOCAL_PATH_TO_CHECK/${LIB_MESOS_FILE_NAME}" ]
	then
		echo "${LIB_MESOS_FILE_NAME} file does not exist. Exiting..."
		exit 1
fi

if [ ! -f "$LOCAL_PATH_TO_CHECK/${JRE_FILE_NAME}" ]
	then
		echo "${JRE_FILE_NAME} file does not exist. Exiting..."
		exit 1
fi

if [ ! -f "$LOCAL_PATH_TO_CHECK/apache-cassandra-${FRAMEWORK_PLUS_CASSANDRA_VERSION}-bin-dcos.tar.gz" ]
	then
		echo "apache-cassandra-${FRAMEWORK_PLUS_CASSANDRA_VERSION}-bin-dcos.tar.gz file does not exist. Exiting..."
		exit 1
fi

if [ ! -f "$LOCAL_PATH_TO_CHECK/bin_wrapper-0.0.1-py2.py3-none-any.whl" ]
	then
		echo "bin_wrapper-0.0.1-py2.py3-none-any.whl file does not exist. Exiting..."
		exit 1
fi

