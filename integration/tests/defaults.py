import os
import shakedown


DEFAULT_NODE_COUNT = 3
PACKAGE_NAME = 'cassandra'
TASK_RUNNING_STATE = 'TASK_RUNNING'

DCOS_URL = shakedown.run_dcos_command('config show core.dcos_url')[0].strip()
OPTIONS_FILE = os.environ.get('OPTIONS_FILE')

IS_STRICT = bool(os.environ.get('STRICT_MODE', 'False'))
PRINCIPAL = os.environ.get('FRAMEWORK_PRINCIPAL', 'cassandra-principal')
