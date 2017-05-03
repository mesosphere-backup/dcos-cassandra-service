#!/bin/bash

CLI_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $CLI_DIR

if [ -z "$GOPATH" -o -z "$(which go)" ]; then
  echo "Missing GOPATH environment variable or 'go' executable. Please configure a Go build environment."
  exit 1
fi


REPO_NAME=dcos-cassandra-service
ORG_PATH=${ORG_PATH:=github.com/mesosphere}
GOPATH_ORG="$GOPATH/src/$ORG_PATH"
GOPATH_EXE_DIR="$GOPATH_ORG/$REPO_NAME/cli/dcos-cassandra"

# Add symlink from GOPATH which points into the repository directory:
SYMLINK_LOCATION="$GOPATH_ORG/$REPO_NAME"

if [ ! -h "$SYMLINK_LOCATION" -o "$(readlink $SYMLINK_LOCATION)" != "$REPO_ROOT_DIR" ]; then
    echo "Creating symlink from GOPATH=$SYMLINK_LOCATION to REPOPATH=$REPO_ROOT_DIR"
    rm -rf "$SYMLINK_LOCATION"
    mkdir -p "$GOPATH_ORG"
    cd $GOPATH_ORG
    ln -s "$REPO_ROOT_DIR" $REPO_NAME
fi

# Run 'go get'/'go build' from within GOPATH:
cd $GOPATH_EXE_DIR

# The name of the binary produced by Go:
if [ -z "$EXE_NAME" ]; then
    EXE_NAME="dcos-cassandra"
fi

print_file_and_shasum() {
    # Only show 'file <filename>' if that utility is available: often missing in CI builds.
    if [ -n "$(which file)" ]; then
        file "$1"
    fi
    ls -l "$1"
    echo ""
}

# ---

# this may be omitted in 1.6+, left here for compatibility with 1.5:
export GO15VENDOREXPERIMENT=1

go get

# available GOOS/GOARCH permutations are listed at:
# https://golang.org/doc/install/source#environment

# windows:
GOOS=windows GOARCH=386 go build
if [ $? -ne 0 ]; then exit 1; fi
print_file_and_shasum "${EXE_NAME}.exe"

# osx (static build):
SUFFIX="-darwin"
CGO_ENABLED=0 GOOS=darwin GOARCH=386 go build \
    && mv -vf "${EXE_NAME}" "${EXE_NAME}${SUFFIX}"
if [ $? -ne 0 ]; then exit 1; fi
# don't ever strip the darwin binary: results in a broken/segfaulty binary
print_file_and_shasum "${EXE_NAME}${SUFFIX}"

# linux (static build):
SUFFIX="-linux"
CGO_ENABLED=0 GOOS=linux GOARCH=386 go build \
    && mv -vf "${EXE_NAME}" "${EXE_NAME}${SUFFIX}"
if [ $? -ne 0 ]; then exit 1; fi
case "$OSTYPE" in
    linux*) strip "${EXE_NAME}${SUFFIX}"
esac
print_file_and_shasum "${EXE_NAME}${SUFFIX}"

cd ..

# ---

# python (wraps above binaries for compatibility with 1.7 and universe-2.x):
cd python/

python setup.py bdist_wheel
if [ $? -ne 0 ]; then exit 1; fi
print_file_and_shasum dist/*.whl

cd ..
