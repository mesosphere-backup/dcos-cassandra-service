import dcos.http
import pytest
import shakedown

from . import infinity_commons

from tests.command import (
    cassandra_api_url,
    check_health,
    install,
    uninstall,
)

def setup_module(module):
    uninstall()


def teardown_module(module):
    uninstall()


@pytest.mark.sanity
def test_marathon_unique_hostname():
    install(additional_options = {'service':{'placement_constraint':'hostname:UNIQUE'}})
    check_health()
    plan = infinity_commons.get_and_verify_plan(lambda p: p['status'] == infinity_commons.PlanState.COMPLETE.value)
    assert plan['status'] == infinity_commons.PlanState.COMPLETE.value
    uninstall()


# effectively same as 'hostname:UNIQUE':
@pytest.mark.sanity
def test_marathon_max_one_per_hostname():
    install(additional_options = {'service':{'placement_constraint':'hostname:MAX_PER:1'}})
    check_health()
    plan = infinity_commons.get_and_verify_plan(lambda p: p['status'] == infinity_commons.PlanState.COMPLETE.value)
    assert plan['status'] == infinity_commons.PlanState.COMPLETE.value
    uninstall()


def allow_incomplete_plan(status_code):
    return 200 <= status_code < 300 or status_code == 503


@pytest.mark.sanity
def test_marathon_rack_not_found():
    # install without waiting, since the install should never succeed and a timeout would result in an
    # assertion failure
    install(additional_options = {'service':{'placement_constraint':'rack_id:LIKE:rack-foo-.*'}}, wait=False)
    try:
        check_health(wait_time=60) # long enough for /plan to work and for a node to have been IN_PROGRESS
        assert False, "Should have failed healthcheck"
    except:
        pass # expected to fail, just wanting to wait 30s
    plan = dcos.http.get(cassandra_api_url('plan'), is_success=allow_incomplete_plan).json()
    # check that first node is still (unsuccessfully) looking for a match:
    assert plan['status'] == infinity_commons.PlanState.IN_PROGRESS.value
    assert infinity_commons.filter_phase(plan, "Deploy")['steps'][0]['status'] == 'PENDING'
    uninstall()
