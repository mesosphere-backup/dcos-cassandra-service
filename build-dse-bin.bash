#!/bin/bash

# DESCRIPTION:
#  Creates a customized dse-VERSION-bin-dcos.tar.gz,
#  which includes additional libs that aren't present in the stock version.
# USAGE:
#  $ ./build-dse-bin.bash
#  [...]
#  Created: /path/to/dcos-cassandra-service/cassandra-bin-tmp/dse-VERSION-bin-dcos.tar.gz
#  Summary of lib/ changes:
#  [... diff between original dse-VERSION-bin-dcos.tar.gz and new dse-VERSION-bin-dcos.tar.gz ... ]
# CUSTOMIZATION:
#  See variables below, in particular DSE_VERSION for bumping the version of cassandra to package.

set -o errexit -o nounset -o pipefail

# VERSION SETTINGS
DSE_VERSION="5.0.2"
METRICS_INTERFACE_VERSION="3" # Cassandra 2.2+ uses metrics3, while <= 2.1 uses metrics2.
STATSD_REPORTER_VERSION="4.1.2"
REPORTER_CONFIG_VERSION="3.0.3"
SEED_PROVIDER_VERSION="1.0.16"
READYTALK_MVN_REPO_DOWNLOAD_URL="https://dl.bintray.com/readytalk/maven/com/readytalk"
MVN_CENTRAL_DOWNLOAD_URL="https://repo1.maven.org/maven2"

# PATHS AND FILENAME SETTINGS
DSE_DIST_NAME="dse-${DSE_VERSION}"
DSE_STOCK_IMAGE="${DSE_DIST_NAME}-bin.tar.gz"
DSE_CUSTOM_IMAGE="${DSE_DIST_NAME}-bin-dcos.tar.gz"
DSE_STOCK_IMAGE_DOWNLOAD_URL="http://downloads.datastax.com/enterprise/$DSE_STOCK_IMAGE"

function _sha1sum {
    # Try 'sha1sum' (Linux) with fallback to 'shasum' (OSX)
    SHA1SUM_EXE=$(which sha1sum || which shasum)
    $SHA1SUM_EXE $@
}

function _download {
    if [ ! -f "$2" ]; then
        wget --progress=dot -e dotbytes=1M -O $2 $1
        _sha1sum $2
    fi
}

function _download_dse {
  curl --user $DSE_USERNAME:$DSE_PASSWORD -O $DSE_STOCK_IMAGE_DOWNLOAD_URL
}

###
# Build seedprovider jar
###

echo "##### Build seedprovider jar #####"
./gradlew :seedprovider:jar

###
# Go into tmp dir
###

mkdir -p "cassandra-bin-tmp"
cd "cassandra-bin-tmp"

###
# Download and unpack stock dse-bin
###

_download_dse

rm -rf $DSE_DIST_NAME
tar xzf $DSE_STOCK_IMAGE

###
# Modify stock cassandra config
###

CONF_DIR="${DSE_DIST_NAME}/resources/cassandra/conf"
_sha1sum "${CONF_DIR}"/* &> confdir-before.txt || true

# Remove default cassandra-rackdc.properties and cassandra-topology.properties.
# The framework will provide this configuration on-the-fly.

echo "##### Remove cassandra-[rackdc|topology].properties #####"

rm -v "${CONF_DIR}"/cassandra-rackdc.properties
rm -v "${CONF_DIR}"/cassandra-topology.properties

# Make a copy of cassandra-env.sh named cassandra-env.sh.bak, then comment out any JMX_PORT
# configuration. This value is customized by the framework.

echo "##### Disable JMX_PORT in cassandra-env.sh #####"

# "JMX_PORT=???" => "#DISABLED FOR DC/OS\n#JMX_PORT=???"
sed -i "s/\(^JMX_PORT=.*\)/#DISABLED FOR DC\/OS:\n#\1/g" "${CONF_DIR}"/cassandra-env.sh

_sha1sum "${CONF_DIR}"/* &> confdir-after.txt || true

###
# Copy seed provider and statsd libraries into cassandra-bin's lib dir
###

LIB_DIR="${DSE_DIST_NAME}/resources/cassandra/lib"
_sha1sum "${LIB_DIR}"/*.jar &> libdir-before.txt || true

echo "##### Install custom seed provider #####"

# Copy seedprovider lib from regular local build into cassandra-bin/lib
cp -v ../seedprovider/build/libs/seedprovider-${SEED_PROVIDER_VERSION}.jar ${LIB_DIR}

echo "##### Install StatsD metrics support #####"

# StatsD Reporter: add stock libraries from maven repo
_download "${READYTALK_MVN_REPO_DOWNLOAD_URL}/metrics${METRICS_INTERFACE_VERSION}-statsd/${STATSD_REPORTER_VERSION}/metrics${METRICS_INTERFACE_VERSION}-statsd-${STATSD_REPORTER_VERSION}.jar" "metrics${METRICS_INTERFACE_VERSION}-statsd-${STATSD_REPORTER_VERSION}.jar"
cp -v "metrics${METRICS_INTERFACE_VERSION}-statsd-${STATSD_REPORTER_VERSION}.jar" ${LIB_DIR}
_download "${READYTALK_MVN_REPO_DOWNLOAD_URL}/metrics-statsd-common/${STATSD_REPORTER_VERSION}/metrics-statsd-common-${STATSD_REPORTER_VERSION}.jar" "metrics-statsd-common-${STATSD_REPORTER_VERSION}.jar"
cp -v "metrics-statsd-common-${STATSD_REPORTER_VERSION}.jar" ${LIB_DIR}

_download "${MVN_CENTRAL_DOWNLOAD_URL}/com/addthis/metrics/reporter-config${METRICS_INTERFACE_VERSION}/${REPORTER_CONFIG_VERSION}/reporter-config${METRICS_INTERFACE_VERSION}-${REPORTER_CONFIG_VERSION}.jar" "reporter-config${METRICS_INTERFACE_VERSION}-${REPORTER_CONFIG_VERSION}.jar"

_download "${MVN_CENTRAL_DOWNLOAD_URL}/com/addthis/metrics/reporter-config-base/${REPORTER_CONFIG_VERSION}/reporter-config-base-${REPORTER_CONFIG_VERSION}.jar" "reporter-config-base-${REPORTER_CONFIG_VERSION}.jar"

rm -vf "${LIB_DIR}"/reporter-config*.jar
cp -v \
   "reporter-config-base-${REPORTER_CONFIG_VERSION}.jar" \
   "${LIB_DIR}/reporter-config-base-${REPORTER_CONFIG_VERSION}.jar"
cp -v \
   "reporter-config${METRICS_INTERFACE_VERSION}-${REPORTER_CONFIG_VERSION}.jar" \
   "${LIB_DIR}/reporter-config${METRICS_INTERFACE_VERSION}-${REPORTER_CONFIG_VERSION}.jar"

_sha1sum "${LIB_DIR}"/*.jar &> libdir-after.txt || true

###
# Rebuild cassandra-bin package with config and library changes
###

rm -vf ${DSE_CUSTOM_IMAGE}
tar czf ${DSE_CUSTOM_IMAGE} ${DSE_DIST_NAME}

echo ""
echo "#####"
echo ""
echo "CREATED: $(pwd)/${DSE_CUSTOM_IMAGE}"
echo ""
pwd
_sha1sum ${DSE_DIST_NAME}*.tar.gz
ls -l ${DSE_DIST_NAME}*.tar.gz
echo ""
echo "Summary of conf/ changes:"
diff confdir-before.txt confdir-after.txt || true
echo ""
echo "Summary of lib/ changes:"
diff libdir-before.txt libdir-after.txt || true
