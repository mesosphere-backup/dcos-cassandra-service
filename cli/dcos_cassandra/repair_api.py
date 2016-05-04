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

from dcos import http
from dcos_cassandra import cassandra_utils as cu


def status():
    return cu.to_json(http.get(cu.api_url("/plan"), headers={}))


def start_repair(nodes, keyspaces=None, column_families=None):
    req = {'nodes': nodes}
    if keyspaces is not None:
        req['key_spaces'] = keyspaces
    if column_families is not None:
        req['column_families'] = column_families
    return http.put(cu.api_url("/repair/start"),
                    json=req,
                    headers={'Content-Type': 'application/json'})
