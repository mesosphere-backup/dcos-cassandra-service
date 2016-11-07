import json
import time
from functools import wraps

import dcos
import shakedown

from tests.defaults import (
    DEFAULT_NODE_COUNT,
    DEFAULT_OPTIONS_DICT,
    PACKAGE_NAME,
    PRINCIPAL,
    TASK_RUNNING_STATE,
)


WAIT_TIME_IN_SECONDS = 600


def as_json(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return json.loads(fn(*args, **kwargs))
        except ValueError:
            return None

    return wrapper


def cassandra_api_url(basename, app_id='cassandra'):
    return '{}/v1/{}'.format(shakedown.dcos_service_url(app_id), basename)


def check_health():
    def fn():
        return shakedown.get_service_tasks(PACKAGE_NAME)

    def success_predicate(tasks):
        running_tasks = [t for t in tasks if t['state'] == TASK_RUNNING_STATE]
        print('Waiting for {} healthy tasks, got {}/{}'.format(
            DEFAULT_NODE_COUNT, len(running_tasks), len(tasks)))
        return (
            len(running_tasks) == DEFAULT_NODE_COUNT,
            'Service did not become healthy'
        )

    return spin(fn, success_predicate)


def get_cassandra_config():
    response = request(
        dcos.http.get,
        marathon_api_url('apps/{}/versions'.format(PACKAGE_NAME))
    )
    assert response.status_code == 200, 'Marathon versions request failed'

    version = response.json()['versions'][0]

    response = dcos.http.get(marathon_api_url('apps/{}/versions/{}'.format(PACKAGE_NAME, version)))
    assert response.status_code == 200

    config = response.json()
    del config['uris']
    del config['version']

    return config


@as_json
def get_dcos_command(command):
    result, error = shakedown.run_dcos_command(command)
    if error:
        raise RuntimeError(
            'command dcos {} {} failed'.format(command, PACKAGE_NAME)
        )

    return result


def marathon_api_url(basename):
    return '{}/v2/{}'.format(shakedown.dcos_service_url('marathon'), basename)


def request(request_fn, *args, **kwargs):
    def success_predicate(response):
        return (
            response.status_code in [200, 202],
            'Request failed: %s' % response.content,
        )

    return spin(request_fn, success_predicate, *args, **kwargs)


def spin(fn, success_predicate, *args, **kwargs):
    now = time.time()
    end_time = now + WAIT_TIME_IN_SECONDS
    while now < end_time:
        print("%s: %.01fs left" % (time.strftime("%H:%M:%S %Z", time.localtime(now)), end_time - now))
        result = fn(*args, **kwargs)
        is_successful, error_message = success_predicate(result)
        if is_successful:
            print('Success state reached, exiting spin.')
            break
        print('Waiting for success state... err={}'.format(error_message))
        time.sleep(1)
        now = time.time()

    assert is_successful, error_message

    return result


def install(additional_options = {}):
    merged_options = _nested_dict_merge(DEFAULT_OPTIONS_DICT, additional_options)
    print('Installing {} with options: {}'.format(PACKAGE_NAME, merged_options))
    shakedown.install_package_and_wait(PACKAGE_NAME, options_json=merged_options)


def uninstall():
    print('Uninstalling/janitoring {}'.format(PACKAGE_NAME))
    try:
        shakedown.uninstall_package_and_wait(PACKAGE_NAME, app_id=PACKAGE_NAME)
    except (dcos.errors.DCOSException, ValueError) as e:
        print('Got exception when uninstalling package, continuing with janitor anyway: {}'.format(e))

    shakedown.run_command_on_master(
        'docker run mesosphere/janitor /janitor.py '
        '-v -r cassandra-role -p {} -z dcos-service-cassandra '
        '--auth_token={}'.format(
            PRINCIPAL,
            shakedown.run_dcos_command(
                'config show core.dcos_acs_token'
            )[0].strip()
        )
    )


def unset_ssl_verification():
    shakedown.run_dcos_command('config set core.ssl_verify false')


def _nested_dict_merge(a, b, path=None):
    "ripped from http://stackoverflow.com/questions/7204805/dictionaries-of-dictionaries-merge"
    if path is None: path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                _nested_dict_merge(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass # same leaf value
            else:
                raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a
