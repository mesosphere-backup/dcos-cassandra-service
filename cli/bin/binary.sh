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

echo "Building binary..."
pyinstaller binary/binary.spec

if [ "$(uname)" == "Darwin" ]; then
    # Do something under Mac OS X platform
    mkdir -p dist/darwin
    mv dist/dcos-cassandra dist/darwin
    echo "Darin Build... building linux"

fi

echo "Build finished!"
