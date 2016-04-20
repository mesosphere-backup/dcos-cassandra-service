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


def start_restore(name, external_location, s3_access_key, s3_secret_key):
    req = {
        'backup_name': name,
        'external_location': external_location,
        's3_access_key': s3_access_key,
        's3_secret_key': s3_secret_key
    }
    return http.put(cu.api_url("/restore/start"),
                    json=req,
                    headers={'Content-Type': 'application/json'})
