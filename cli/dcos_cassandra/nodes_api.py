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


def list():
    return cu.to_json(http.get(cu.api_url("/nodes/list"), headers={}))


def describe(id):
    url = "/nodes/node-{}/info".format(str(id))
    return cu.to_json(http.get(cu.api_url(url), headers={}))


def status(id):
    url = "/nodes/node-{}/status".format(str(id))
    return cu.to_json(http.get(cu.api_url(url), headers={}))


def restart(id):
    url = "/nodes/restart?node=node-{}".format(str(id))
    response = http.put(cu.api_url(url), headers={})
    return response.status_code % 200 < 100


def replace(id):
    url = "/nodes/replace?node=node-{}".format(str(id))
    response = http.put(cu.api_url(url), headers={})
    return response.status_code % 200 < 100


def connection():
    return cu.to_json(http.get(cu.api_url("/nodes/connect"), headers={}))


def connection_address():
    return cu.to_json(http.get(cu.api_url("/nodes/connect/address"),
                               headers={}))


def connection_dns():
    return cu.to_json(http.get(cu.api_url("/nodes/connect/dns"),
                               headers={}))
