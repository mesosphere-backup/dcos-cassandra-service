import shakedown


DEFAULT_NODE_COUNT = 3
PACKAGE_NAME = 'cassandra'
TASK_RUNNING_STATE = 'TASK_RUNNING'


_request_headers = None
def request_headers():
    global _request_headers

    if not _request_headers:
        _request_headers = {
            'authorization': 'token=%s' % (
                shakedown.run_dcos_command(
                    'config show core.dcos_acs_token'
                )[0].strip()
            ),
        }

    return _request_headers
