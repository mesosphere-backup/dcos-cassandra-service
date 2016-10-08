import json
import shakedown
import dcos
from dcos import marathon
from enum import Enum
from tests.command import (
    cassandra_api_url,
    spin
)



class PlanState(Enum):
    ERROR = "ERROR"
    WAITING = "WAITING"
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETE = "COMPLETE"


def filter_phase(plan, phase_name):
    for phase in plan['phases']:
        if phase['name'] == phase_name:
            return phase

    return None


def get_phase_index(plan, phase_name):
    idx = 0
    for phase in plan['phases']:
        if phase['name'] == phase_name:
            return idx
        else:
            idx += 1
    return -1


counter = 0
def get_and_verify_plan(predicate=lambda r: True):
    global counter
    plan_url = cassandra_api_url('plan')
    def fn():
        try:
            return dcos.http.get(plan_url)
        except dcos.errors.DCOSHTTPException as err:
            return err.response

    def success_predicate(result):
        global counter
        message = 'Request to {} failed'.format(plan_url)

        try:
            body = result.json()
        except ValueError:
            return False, message

        if counter < 3:
            counter += 1
        pred_res = predicate(body)
        if pred_res:
            counter = 0

        return pred_res, message

    return spin(fn, success_predicate).json()


def get_marathon_uri():
    """Gets URL to the Marathon instance"""
    return '{}/marathon'.format(shakedown.dcos_url())


def get_marathon_client():
    """Gets a marathon client"""
    return marathon.Client(get_marathon_uri())


def strip_meta(app):
    app.pop('fetch')
    app.pop('version')
    app.pop('versionInfo')
    return app
