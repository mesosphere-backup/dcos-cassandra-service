import os
import shakedown


DEFAULT_NODE_COUNT = 3
PACKAGE_NAME = 'cassandra'
TASK_RUNNING_STATE = 'TASK_RUNNING'

DCOS_URL = shakedown.run_dcos_command('config show core.dcos_url')[0].strip()

# expected SECURITY values: 'permissive', 'strict', 'disabled'
if os.environ.get('SECURITY', '') == 'strict':
    print('Using strict mode test configuration')
    PRINCIPAL = 'service-acct'
    DEFAULT_OPTIONS_DICT = {
        "service": {
            "user": "nobody",
            "principal": PRINCIPAL,
            "secret_name": "secret"
        }
    }
else:
    print('Using default test configuration')
    PRINCIPAL = 'cassandra-principal'
    DEFAULT_OPTIONS_DICT = {}
