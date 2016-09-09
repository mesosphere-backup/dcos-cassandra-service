#!/usr/bin/env bash

# Exit immediately on errors -- the helper scripts all emit github statuses internally
set -e

REPO_ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $REPO_ROOT_DIR

# Grab dcos-commons build/release tools:
rm -rf dcos-commons-tools/ && curl https://infinity-artifacts.s3.amazonaws.com/dcos-commons-tools.tgz | tar xz

# Get a CCM cluster if not already configured (see available settings in dcos-commons/tools/README.md):
if [ -z "$CLUSTER_URL" ]; then
    echo "CLUSTER_URL is empty/unset, launching new cluster."
    CCM_AGENTS=5
    CLUSTER_ID_URL=$(./dcos-commons-tools/launch_ccm_cluster.py)
    export CLUSTER_URL=$(echo "${CLUSTER_ID_URL}" | jq .url)
    export CLUSTER_ID=$(echo "${CLUSTER_ID_URL}" | jq .id)
else
    echo "Using provided CLUSTER_URL as cluster: $CLUSTER_URL"
fi

# Run shakedown tests:
${REPO_ROOT_DIR}/dcos-commons-tools/run_tests.py shakedown ${REPO_ROOT_DIR}/integration/tests/ ${REPO_ROOT_DIR}/integration/requirements.txt

# Run legacy dcos-tests:
if [ -d "${REPO_ROOT_DIR}/dcos-tests" ]; then
    ${REPO_ROOT_DIR}/dcos-commons-tools/run_tests.py dcos-tests ${REPO_ROOT_DIR}/dcos-tests/infinitytests/kafka ${REPO_ROOT_DIR}/dcos-tests/
else
    echo "${REPO_ROOT_DIR}/dcos-tests/ not found, skipping dcos-tests"
fi
