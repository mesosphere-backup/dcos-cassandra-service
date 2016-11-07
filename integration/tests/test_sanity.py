import json
import dcos
import pytest
import time

import dcos
import shakedown

from . import infinity_commons

from tests.command import (
    cassandra_api_url,
    check_health,
    install,
    uninstall,
    unset_ssl_verification,
)
from tests.defaults import DEFAULT_NODE_COUNT, PACKAGE_NAME


# install once up-front, reuse install for tests (MUCH FASTER):
def setup_module():
    unset_ssl_verification()

    uninstall()
    install()
    check_health()


def teardown_module():
    uninstall()


@pytest.mark.sanity
def test_connect():
    result = dcos.http.get(cassandra_api_url('connection'))
    body = result.json()
    assert len(body) == 2
    assert len(body["address"]) == DEFAULT_NODE_COUNT
    assert len(body["dns"]) == DEFAULT_NODE_COUNT


@pytest.mark.sanity
def test_connect_address():
    result = dcos.http.get(cassandra_api_url('connection/address'))
    body = result.json()
    assert len(body) == DEFAULT_NODE_COUNT


@pytest.mark.sanity
def test_connect_dns():
    result = dcos.http.get(cassandra_api_url('connection/dns'))
    body = result.json()
    assert len(body) == DEFAULT_NODE_COUNT


@pytest.mark.sanity
def test_install():
    completed_plan = infinity_commons.get_and_verify_plan(lambda p: p['status'] == infinity_commons.PlanState.COMPLETE.value)
    assert completed_plan['status'] == infinity_commons.PlanState.COMPLETE.value


@pytest.mark.sanity
def test_env_unchanged():
    completed_plan = infinity_commons.get_and_verify_plan(lambda p: p['status'] == infinity_commons.PlanState.COMPLETE.value)
    assert completed_plan['status'] == infinity_commons.PlanState.COMPLETE.value

    mc = dcos.marathon.create_client()
    app = mc.get_app('/cassandra')
    app = infinity_commons.strip_meta(app)
    mc.update_app(app_id='/cassandra', payload=app)
    completed_plan = infinity_commons.get_and_verify_plan(lambda p: p['status'] == infinity_commons.PlanState.COMPLETE.value)
    assert completed_plan['status'] == infinity_commons.PlanState.COMPLETE.value


@pytest.mark.sanity
def test_nodes_increase_by_one():
    completed_plan = infinity_commons.get_and_verify_plan(lambda p: p['status'] == infinity_commons.PlanState.COMPLETE.value)
    mc = dcos.marathon.create_client()
    app = mc.get_app('/cassandra')
    app = infinity_commons.strip_meta(app)
    oe = app['env']
    env_node_count = int(oe['NODES']) + 1
    oe['NODES'] = str(env_node_count)
    app['env'] = oe
    print("Updated node count: {}".format(app['env']['NODES']))
    print(mc.update_app(app_id='/cassandra', payload=app, force=True))
    check_health()
    plan = infinity_commons.get_and_verify_plan(lambda p: p['status'] == infinity_commons.PlanState.COMPLETE.value and len(infinity_commons.filter_phase(p, "Deploy")['blocks']) == 4 and infinity_commons.filter_phase(p, "Deploy")['blocks'][env_node_count - 1]['status'] == infinity_commons.PlanState.COMPLETE.value)
    print(plan)
    assert plan['status'] == infinity_commons.PlanState.COMPLETE.value
    # reinstall after increase:
    uninstall()
    install()
    check_health()


@pytest.mark.sanity
def test_nodes_decrease_by_one_should_fail():
    completed_plan = infinity_commons.get_and_verify_plan(
        lambda p: p['status'] == infinity_commons.PlanState.COMPLETE.value)
    mc = dcos.marathon.create_client()
    app = mc.get_app('/cassandra')
    app = infinity_commons.strip_meta(app)
    oe = app['env']
    env_node_count = int(oe['NODES']) - 1
    oe['NODES'] = str(env_node_count)
    app['env'] = oe
    print("Updated node count: {}".format(app['env']['NODES']))
    print(mc.update_app(app_id='/cassandra', payload=app, force=True))
    check_health()
    plan = infinity_commons.get_and_verify_plan(
        lambda p: (p['status'] == infinity_commons.PlanState.ERROR.value and
                   len(infinity_commons.filter_phase(p, "Deploy")['blocks']) == 3))
    print(plan)
    assert plan['status'] == infinity_commons.PlanState.ERROR.value

    # Revert
    oe = app['env']
    env_node_count = int(oe['NODES']) + 1
    oe['NODES'] = str(env_node_count)
    app['env'] = oe
    print("Reverted node count: {}".format(app['env']['NODES']))
    print(mc.update_app(app_id='/cassandra', payload=app, force=True))
    check_health()
    plan = infinity_commons.get_and_verify_plan(
        lambda p: (p['status'] == infinity_commons.PlanState.COMPLETE.value) and
        (len(infinity_commons.filter_phase(p, "Deploy")['blocks']) == 3))
    print(plan)
    assert plan['status'] == infinity_commons.PlanState.COMPLETE.value


@pytest.mark.sanity
def test_change_disk_should_fail():
    completed_plan = infinity_commons.get_and_verify_plan(lambda p: p['status'] == infinity_commons.PlanState.COMPLETE.value)
    mc = dcos.marathon.create_client()
    app = mc.get_app('/cassandra')
    app = infinity_commons.strip_meta(app)
    oe = app['env']
    disk = int(oe['CASSANDRA_DISK_MB']) - 1
    oe['CASSANDRA_DISK_MB'] = str(disk)
    app['env'] = oe
    print("Updated CASSANDRA_DISK_MB: {}".format(app['env']['CASSANDRA_DISK_MB']))
    print(mc.update_app(app_id='/cassandra', payload=app, force=True))
    check_health()
    plan = infinity_commons.get_and_verify_plan(lambda p: p['status'] == infinity_commons.PlanState.ERROR.value)
    print(plan)
    assert plan['status'] == infinity_commons.PlanState.ERROR.value

    # Revert
    oe = app['env']
    disk = int(oe['CASSANDRA_DISK_MB']) + 1
    oe['CASSANDRA_DISK_MB'] = str(disk)
    app['env'] = oe
    print("Reverted CASSANDRA_DISK_MB: {}".format(app['env']['CASSANDRA_DISK_MB']))
    print(mc.update_app(app_id='/cassandra', payload=app, force=True))
    check_health()
    plan = infinity_commons.get_and_verify_plan(lambda p: p['status'] == infinity_commons.PlanState.COMPLETE.value)
    print(plan)
    assert plan['status'] == infinity_commons.PlanState.COMPLETE.value


@pytest.mark.sanity
def test_cpus_increase_slightly():
    completed_plan = infinity_commons.get_and_verify_plan(lambda p: p['status'] == infinity_commons.PlanState.COMPLETE.value)
    mc = dcos.marathon.create_client()
    app = mc.get_app('/cassandra')
    app = infinity_commons.strip_meta(app)
    oe = app['env']
    cpu = float(oe['CASSANDRA_CPUS']) + 0.1
    oe['CASSANDRA_CPUS'] = str(cpu)
    app['env'] = oe
    print("Updated CASSANDRA_CPUS: {}".format(app['env']['CASSANDRA_CPUS']))
    print(mc.update_app(app_id='/cassandra', payload=app, force=True))
    check_health()
    plan = infinity_commons.get_and_verify_plan(lambda p: p['status'] == infinity_commons.PlanState.COMPLETE.value and len(infinity_commons.filter_phase(p, "Deploy")['blocks']) == 3)
    print(plan)
    assert plan['status'] == infinity_commons.PlanState.COMPLETE.value


@pytest.mark.sanity
def test_is_suppressed():
    infinity_commons.get_and_verify_plan(lambda p: p['status'] == 'COMPLETE')
    time.sleep(5)
    response = dcos.http.get(cassandra_api_url('state/properties/suppressed'))
    response.raise_for_status()
    assert response.text == "true"
