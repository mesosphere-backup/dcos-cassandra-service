#!/bin/bash -e
#
#    Copyright (C) 2015 Mesosphere, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

BASEDIR=`dirname $0`/..

if [ ! -d "$BASEDIR/env" ]; then
    virtualenv -q $BASEDIR/env --prompt='(dcos-datastax-cli) '
    echo "Virtualenv created."
fi

cd $BASEDIR
uname -a
if [ "$(uname)" = "Darwin" -o "$(expr substr $(uname -s) 1 5)" = "Linux" ]; then
    source $BASEDIR/env/bin/activate
    echo "Virtualenv activated."
else
    # apparently not needed on Windows
    echo "Skipping virtualenv activation."
fi

cd pkg-datastax
# hack: create temp copies for packaging (somehow symlinks end up going recursive, something in python?)
rm -rf dcos_cassandra/ README.rst ../dcos_cassandra/__pycache__/
if [ "$(uname -s)" = "Windows_NT" ]; then
    echo "Ignoring failure return code:"
    cp -av ../dcos_cassandra/ . || true # windows returns an error code here, even on success??
else
    cp -av ../dcos_cassandra/ .
fi
cp -v ../README.rst .

echo "Building datastax wheel..."
python setup.py bdist_wheel

echo "Building datastax egg..."
python setup.py sdist

# clean up copies:
rm -rf dcos_cassandra/ README.rst
