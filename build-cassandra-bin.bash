#!/bin/bash

# DESCRIPTION:
#  Creates a customized apache-cassandra-VERSION-bin.tar.gz,
#  which includes additional libs that aren't present in the stock version.
# USAGE:
#  $ ./build-cassandra-bin.bash
#  [...]
#  Created: /path/to/dcos-cassandra-service/cassandra-bin-tmp/apache-cassandra-2.2.5-bin-dcos.tar.gz
#  Summary of lib/ changes:
#  [... diff between original apache-cassandra-x.y.z-bin.tar.gz and new apache-cassandra-x.y.z-bin-dcos.tar.gz ... ]
# CUSTOMIZATION:
#  See variables below, in particular CASSANDRA_VERSION for bumping the version of cassandra to package.

set -o errexit -o nounset -o pipefail

# VERSION SETTINGS
CASSANDRA_VERSION="2.2.5"
METRICS_INTERFACE_VERSION="3" # Cassandra 2.2+ uses metrics3, while <= 2.1 uses metrics2.
STATSD_REPORTER_VERSION="4.1.2"
REPORTER_CONFIG_VERSION_IN="3.0.1-SNAPSHOT"
REPORTER_CONFIG_SHA1="595b3c239e2c4764c66d214837005a8e0fe01d99"
REPORTER_CONFIG_VERSION_OUT="3.0.1-${REPORTER_CONFIG_SHA1:0:8}" # get first 8 chars of sha
SEED_PROVIDER_VERSION="0.1.0"

# PATHS AND FILENAME SETTINGS
CASSANDRA_DIST_NAME="apache-cassandra-${CASSANDRA_VERSION}"
CASSANDRA_STOCK_IMAGE="${CASSANDRA_DIST_NAME}-bin.tar.gz"
CASSANDRA_CUSTOM_IMAGE="${CASSANDRA_DIST_NAME}-bin-dcos.tar.gz"
CASSANDRA_STOCK_IMAGE_DOWNLOAD_URL="https://archive.apache.org/dist/cassandra/${CASSANDRA_VERSION}/${CASSANDRA_STOCK_IMAGE}"

function _download {
    if [ ! -f "$2" ]; then
        wget --progress=dot -e dotbytes=1M -O $2 $1
        sha1sum $2
    fi
}

function _package_github {
    PROJECT_NAME=$(basename $1) # user/pkg => pkg
    if [ ! -f "$PROJECT_NAME/$3" ]; then
        echo "Building $PROJECT_NAME/$3 from $1:$2"
        if [ ! -d "$PROJECT_NAME" ]; then
            time git clone "https://github.com/$1" --depth 1 && cd "$PROJECT_NAME" && git reset --hard $2
        else
            cd "$PROJECT_NAME"
        fi
        time mvn -Dmaven.test.skip=true package
        cd ..
    fi
}

mkdir -p "cassandra-bin-tmp"
cd "cassandra-bin-tmp"

# Download and unpack stock cassandra-bin and verify with sha1 file

_download $CASSANDRA_STOCK_IMAGE_DOWNLOAD_URL.sha1 "${CASSANDRA_STOCK_IMAGE}.sha1"
_download $CASSANDRA_STOCK_IMAGE_DOWNLOAD_URL "${CASSANDRA_STOCK_IMAGE}"
if [ "$(sha1sum $CASSANDRA_STOCK_IMAGE | awk '{print $1}')" != "$(cat ${CASSANDRA_STOCK_IMAGE}.sha1)" ]; then
    echo "SHA1 MISMATCH: ${CASSANDRA_STOCK_IMAGE}"
    exit 1
else
    echo "Verified: ${CASSANDRA_STOCK_IMAGE}"
fi
rm -rf $CASSANDRA_DIST_NAME
tar xzf $CASSANDRA_STOCK_IMAGE

LIB_DIR="${CASSANDRA_DIST_NAME}/lib"

sha1sum "${LIB_DIR}"/*.jar &> libdir-before.txt || true

# Copy libraries into cassandra-bin's lib dir

echo "##### Seed Provider #####"

# Copy seedprovider lib from regular local build into cassandra-bin/lib
cp -v ../seedprovider/build/libs/seedprovider-${SEED_PROVIDER_VERSION}.jar ${LIB_DIR}

echo "##### StatsD Metrics #####"

# StatsD Reporter: add stock libs from maven repo
_download "https://dl.bintray.com/readytalk/maven/com/readytalk/metrics${METRICS_INTERFACE_VERSION}-statsd/${STATSD_REPORTER_VERSION}/metrics${METRICS_INTERFACE_VERSION}-statsd-${STATSD_REPORTER_VERSION}.jar" "metrics${METRICS_INTERFACE_VERSION}-statsd-${STATSD_REPORTER_VERSION}.jar"
cp -v "metrics${METRICS_INTERFACE_VERSION}-statsd-${STATSD_REPORTER_VERSION}.jar" ${LIB_DIR}
_download "https://dl.bintray.com/readytalk/maven/com/readytalk/metrics-statsd-common/${STATSD_REPORTER_VERSION}/metrics-statsd-common-${STATSD_REPORTER_VERSION}.jar" "metrics-statsd-common-${STATSD_REPORTER_VERSION}.jar"
cp -v "metrics-statsd-common-${STATSD_REPORTER_VERSION}.jar" ${LIB_DIR}

# Metrics Config Parser library: build custom version with added statsd support (replaces cassandra-bin's version which currently lacks statsd)

_package_github "addthis/metrics-reporter-config" "${REPORTER_CONFIG_SHA1}" "reporter-config${METRICS_INTERFACE_VERSION}/target/reporter-config${METRICS_INTERFACE_VERSION}-${REPORTER_CONFIG_VERSION_IN}.jar"

rm -vf "${LIB_DIR}"/reporter-config*.jar

cp -v \
   "metrics-reporter-config/reporter-config-base/target/reporter-config-base-${REPORTER_CONFIG_VERSION_IN}.jar" \
   "${LIB_DIR}/reporter-config-base-${REPORTER_CONFIG_VERSION_OUT}.jar"

cp -v \
   "metrics-reporter-config/reporter-config${METRICS_INTERFACE_VERSION}/target/reporter-config${METRICS_INTERFACE_VERSION}-${REPORTER_CONFIG_VERSION_IN}.jar" \
   "${LIB_DIR}/reporter-config${METRICS_INTERFACE_VERSION}-${REPORTER_CONFIG_VERSION_OUT}.jar"

sha1sum "${LIB_DIR}"/*.jar &> libdir-after.txt || true

# Repack cassandra-bin package with library changes

rm -vf ${CASSANDRA_CUSTOM_IMAGE}
tar czf ${CASSANDRA_CUSTOM_IMAGE} ${CASSANDRA_DIST_NAME}

echo ""
echo "#####"
echo ""
echo "CREATED: $(pwd)/${CASSANDRA_CUSTOM_IMAGE}"
echo ""
sha1sum ${CASSANDRA_DIST_NAME}*.tar.gz
ls -l ${CASSANDRA_DIST_NAME}*.tar.gz
echo ""
echo "Summary of lib/ changes:"
diff libdir-before.txt libdir-after.txt || true
