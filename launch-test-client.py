#!/usr/bin/python

'''Launches cassandra-stress instances in Marathon.'''

import getopt
import json
import logging
import pprint
import random
import requests
from requests.exceptions import HTTPError
import string
import sys
import urllib
import urlparse

def __urljoin(*elements):
    return "/".join(elem.strip("/") for elem in elements)

def __post(url, json=None):
    pprint.pprint(json)
    return __handle_response("POST", url, requests.post(url, json=json))

def __handle_response(httpcmd, url, response):
    # http code 200-299 => success!
    if response.status_code < 200 or response.status_code >= 300:
        errmsg = "Error code in response to %s %s: %s/%s" % (
            httpcmd, url, response.status_code, response.content)
        print(errmsg)
        raise HTTPError(errmsg)
    json = response.json()
    print("Got response for %s %s:\n%s" % (httpcmd, url, json))
    return json

def marathon_apps_url(cluster_uri):
    url = __urljoin(cluster_uri, "marathon", "v2", "apps")
    print("Marathon query: %s" % url)
    return url

def marathon_launch_app(marathon_url, app_id, cmd, instances=1, packages=[], env={}):

    formatted_packages = []
    for package in packages:
        formatted_packages.append({"uri": package})
    post_json = {
        "id": app_id,
        "container": {
            "type": "MESOS",
        },
        "cmd": cmd,
        "cpus": 1,
        "mem": 512.0, # 512m apparently required: 128m and 256m results in FAILEDs.
        "disk": 1,
        "instances": instances,
        "fetch": formatted_packages,
    }

    json = __post(marathon_url, json=post_json)
    return json["deployments"]

def get_random_id(length=8):
    return ''.join([random.choice(string.ascii_lowercase) for _ in range(length)])


FRAMEWORK_NAME_DEFAULT='cassandra'
WRITER_COUNT_DEFAULT = 5
READER_COUNT_DEFAULT = 5
THREAD_COUNT_DEFAULT = 5
DURATION_DEFAULT = '1h'
CONSISTENCY_DEFAULT = 'one'
TRUNCATE_DEFAULT = 'never'
ERROR_THRESHOLD_DEFAULT = '0.02'


JRE_URL = "https://s3-eu-west-1.amazonaws.com/downloads.mesosphere.com/kafka/jre-8u72-linux-x64.tar.gz"
JRE_JAVA_PATH = "jre/bin/java"

PKG_URL_DEFAULT = "https://s3-us-west-2.amazonaws.com/cassandra-framework-dev/testing/apache-cassandra-2.2.5-bin.tar.gz"
CASSANDRA_STRESS_PATH = "apache-cassandra-2.2.5/tools/bin/cassandra-stress"


def print_help(argv):
    print('''Flags: {}
  -h/--help
  -c/--cluster_url=<REQUIRED, eg https://cluster-url.com>
  -f/--framework_name={}
  -i/--writer_count={}
  -o/--reader_count={}
  -t/--thread_count={}
  --duration={}
  --consistency={}
  --truncate={}
  --error_threshold={}
  --keyspace_override=""
  --ip_override=""
  --pkg_url={}
'''.format(
      argv[0],
      FRAMEWORK_NAME_DEFAULT,
      WRITER_COUNT_DEFAULT,
      READER_COUNT_DEFAULT,
      THREAD_COUNT_DEFAULT,
      DURATION_DEFAULT,
      CONSISTENCY_DEFAULT,
      TRUNCATE_DEFAULT,
      ERROR_THRESHOLD_DEFAULT,
      PKG_URL_DEFAULT))
    print('Example: {} -c http://your-cluster-url.com'.format(argv[0]))


def main(argv):
    cluster_url = ''
    framework_name = FRAMEWORK_NAME_DEFAULT
    writer_count = WRITER_COUNT_DEFAULT
    reader_count = READER_COUNT_DEFAULT
    thread_count = THREAD_COUNT_DEFAULT
    duration = DURATION_DEFAULT
    consistency = CONSISTENCY_DEFAULT
    truncate = TRUNCATE_DEFAULT
    error_threshold = ERROR_THRESHOLD_DEFAULT
    keyspace_override = ''
    ip_override = ''
    pkg_url = PKG_URL_DEFAULT

    try:
        opts, args = getopt.getopt(argv[1:], 'hc:f:i:o:t:q:m:s:', [
            'help',
            'cluster_url=',
            'framework_name=',
            'writer_count=',
            'reader_count=',
            'thread_count=',
            'duration=',
            'consistency=',
            'truncate=',
            'error_threshold=',
            'keyspace_override=',
            'ip_override=',
            'pkg_url='])
    except getopt.GetoptError:
        print_help(argv)
        sys.exit(1)
    for opt, arg in opts:
        if opt in ['-h', '--help']:
            print_help(argv)
            sys.exit(1)
        elif opt in ['-c', '--cluster_url']:
            cluster_url = arg.rstrip('/')
        elif opt in ['-f', '--framework_name']:
            framework_name = arg
        elif opt in ['-i', '--writer_count']:
            writer_count = int(arg)
        elif opt in ['-o', '--reader_count']:
            reader_count = int(arg)
        elif opt in ['-t', '--thread_count']:
            thread_count = int(arg)
        elif opt in ['-d', '--duration']:
            duration = arg
        elif opt in ['--consistency']:
            consistency = arg
        elif opt in ['--truncate']:
            truncate = arg
        elif opt in ['--error_threshold']:
            error_threshold = arg
        elif opt in ['--keyspace_override']:
            keyspace_override = arg
        elif opt in ['--ip_override']:
            ip_override = arg
        elif opt in ['--pkg_url']:
            pkg_url = arg

    if not cluster_url:
        print('-c/--cluster_url is required')
        print_help(argv)
        sys.exit(1)

    keyspace_rand_id = get_random_id()
    writer_app_id = 'cassandratest-' + keyspace_rand_id + '-writer'
    reader_app_id = 'cassandratest-' + keyspace_rand_id + '-reader'

    if not ip_override:
        # user didn't manually specify ips, fetch them from the framework directly before proceeding
        fetch_ips_path = '{}/service/{}/v1/nodes/connect/native'.format(cluster_url, framework_name)
        json = __handle_response('GET', fetch_ips_path, requests.get(fetch_ips_path))
        ip_override = ','.join(json)
        print('Using node IPs: {}'.format(json))

    if not keyspace_override:
        # user didn't manually specify keyspace, generate random one (matches marathon job names)
        keyspace_override = 'test-' + keyspace_rand_id

    common_args = '-node {} -schema keyspace={} -rate threads={}'.format(
        ip_override, keyspace_override, thread_count)
    #TODO invalid parameters "duration=1h" "cl=one" "err<0.02" "truncate=never"
    ORIG_common_args = '-node {} -schema keyspace={} -rate threads={} duration={} cl={} err\<{} truncate={}'.format(
        ip_override, keyspace_override, thread_count, duration, consistency, error_threshold, truncate)
    reader_args = "counter_read " + common_args
    writer_args = "counter_write " + common_args

    marathon_url = marathon_apps_url(cluster_url)
    if not marathon_launch_app(
            marathon_url = marathon_url,
            app_id = reader_app_id,
            cmd = "export JAVA_HOME=${MESOS_SANDBOX}/jre && env && ${MESOS_SANDBOX}/%s %s" % (
                CASSANDRA_STRESS_PATH, reader_args),
            instances = reader_count,
            packages = [JRE_URL, pkg_url]):
        print("Starting readers failed, skipping launch of writers")
        return 1
    if not marathon_launch_app(
            marathon_url = marathon_url,
            app_id = writer_app_id,
            cmd = "export JAVA_HOME=${MESOS_SANDBOX}/jre && env && ${MESOS_SANDBOX}/%s %s" % (
                CASSANDRA_STRESS_PATH, writer_args),
            instances = writer_count,
            packages = [JRE_URL, pkg_url]):
        print("Starting writers failed")
        return 1

    print('''#################
Readers/writers have been launched.
When finished, delete them from Marathon with these commands:

curl -X DELETE {}/{}
curl -X DELETE {}/{}'''.format(
    marathon_url, reader_app_id,
    marathon_url, writer_app_id))
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))
