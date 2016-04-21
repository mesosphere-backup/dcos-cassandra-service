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

import json
from dcos import util


__fwk = None


def get_fwk_name():
    return __fwk \
        or util.get_config().get('cassandra.service_name') \
        or "cassandra"


def set_fwk_name(name):
    global __fwk
    __fwk = name


def base_url():
    return util.get_config().get('core.dcos_url').rstrip("/")


def marathon_url(slash_command):
    return "%s/marathon/v2%s" % (base_url(), slash_command)


def api_url(slash_command):
    base_config_url = util.get_config().get('cassandra.url')
    if base_config_url is not None:
        base_config_url = base_config_url.rstrip("/")
    else:
        base_config_url = "%s/service/%s" % (base_url(), get_fwk_name())
    return "%s/v1%s" % (base_config_url, slash_command)


def to_json(responseObj):
    # throw any underlying request error
    responseObj.raise_for_status()
    # return json
    return responseObj.json()


def print_json(jsonObj):
    print(json.dumps(jsonObj,
                     sort_keys=True,
                     indent=4,
                     separators=(',', ': ')))
