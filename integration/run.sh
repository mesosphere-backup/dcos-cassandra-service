#!/usr/bin/env bash


set -x


PROJECT_ROOT="$(pwd)"


notify_github() {(
    cd "$PROJECT_ROOT"
    ./integration/scripts/update-github-status.sh "$1" "$2" "$3"
)}


cd integration

# Launch a dcos cluster. Assume for the moment that we're still cloning the
# dcos-tests repo into our workspace.
virtualenv -p python3 --always-copy dcos-venv
. dcos-venv/bin/activate
pip install --no-cache-dir -r ../dcos-tests/requirements.txt

export FRAMEWORK_TESTS_CHANNEL="testing/master"
TEST_RESULT="success"
python -u ../dcos-tests/launch_dcos_cluster.py || TEST_RESULT="failure"
if [ "$TEST_RESULT" = "failure" ]; then
    echo "Cluster launch failed: exiting early"
    notify_github error "$GITHUB_TEST_TYPE" "Failed to launch DCOS cluster"
    exit 5
fi

DCOS_URL="http://$(jq -r '.master_dns_address' launch-data)"
deactivate

# Install dcos-cli, then piggy-back on its virtualenv.
git clone git@github.com:dcos/dcos-cli.git
pushd .
cd dcos-cli && make env && make packages && cd cli && make env
popd
PATH="$PWD/dcos-cli/cli/env/bin:$PATH"

# Point dcos-cli to our cluster and authenticate.
dcos config set core.dcos_url "$DCOS_URL"
dcos config set core.reporting True
dcos config set core.ssl_verify false
dcos config set core.timeout 5
dcos config show
expect cli-login.expect bootstrapuser deleteme

echo "### Cluster info: $(cat launch-data)"
echo "    => Master URI for tests: $DCOS_URL"

# Install shakedown into our virtualenv.
pip install --no-cache-dir -r requirements.txt
shakedown --dcos-url "$DCOS_URL" --ssh-key-file none tests
