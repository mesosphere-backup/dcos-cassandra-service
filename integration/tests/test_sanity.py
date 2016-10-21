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
    uninstall,
    unset_ssl_verification,
)
from tests.defaults import DEFAULT_NODE_COUNT, OPTIONS_FILE, PACKAGE_NAME


def setup_module():
    unset_ssl_verification()


@pytest.yield_fixture
def install_framework():
    uninstall()
    shakedown.install_package_and_wait(PACKAGE_NAME, options_file=OPTIONS_FILE)
    check_health()
    yield

    uninstall()


@pytest.mark.sanity
def test_connect(install_framework):
    try:
        result = dcos.http.get(cassandra_api_url('connection'))
        try:
            body = result.json()
            assert len(body) == 3
            assert len(body["address"]) == DEFAULT_NODE_COUNT
            assert len(body["dns"]) == DEFAULT_NODE_COUNT
            assert body["vip"] == 'node.{}.l4lb.thisdcos.directory:9042'.format(PACKAGE_NAME)
        except:
            print('Failed to parse connect response')
            raise
    except:
        # TODO: remove fallback when universe has recent build with '/connection'
        result = dcos.http.get(cassandra_api_url('nodes/connect'))
        try:
            body = result.json()
            assert len(body) == 2
            assert len(body["address"]) == DEFAULT_NODE_COUNT
            assert len(body["dns"]) == DEFAULT_NODE_COUNT
        except:
            print('Failed to parse connect response')
            raise


@pytest.mark.sanity
def test_connect_address(install_framework):
    # TODO: remove fallback when universe has recent build with '/connection'
    try:
        result = dcos.http.get(cassandra_api_url('connection/address'))
    except:
        result = dcos.http.get(cassandra_api_url('nodes/connect/address'))

    try:
        body = result.json()
        assert len(body) == DEFAULT_NODE_COUNT
    except:
        print('Failed to parse connect response')
        raise


@pytest.mark.sanity
def test_connect_dns(install_framework):
    # TODO: remove fallback when universe has recent build with '/connection'
    try:
        result = dcos.http.get(cassandra_api_url('connection/dns'))
    except:
        result = dcos.http.get(cassandra_api_url('nodes/connect/dns'))

    try:
        body = result.json()
        assert len(body) == DEFAULT_NODE_COUNT
    except:
        print('Failed to parse connect response')
        raise

        return False


@pytest.mark.sanity
def test_install(install_framework):
    completed_plan = infinity_commons.get_and_verify_plan(lambda p: p['status'] == infinity_commons.PlanState.COMPLETE.value)

    assert completed_plan['status'] == infinity_commons.PlanState.COMPLETE.value


@pytest.mark.sanity
def test_env_unchanged(install_framework):
    completed_plan = infinity_commons.get_and_verify_plan(lambda p: p['status'] == infinity_commons.PlanState.COMPLETE.value)
    assert completed_plan['status'] == infinity_commons.PlanState.COMPLETE.value

    mc = dcos.marathon.create_client()
    app = mc.get_app('/cassandra')
    app = infinity_commons.strip_meta(app)
    mc.update_app(app_id='/cassandra', payload=app)
    completed_plan = infinity_commons.get_and_verify_plan(lambda p: p['status'] == infinity_commons.PlanState.COMPLETE.value)
    assert completed_plan['status'] == infinity_commons.PlanState.COMPLETE.value


@pytest.mark.sanity
def test_nodes_increase_by_one(install_framework):
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


@pytest.mark.sanity
def test_nodes_decrease_by_one_should_fail(install_framework):
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
def test_change_disk_should_fail(install_framework):
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
def test_cpus_increase_slightly(install_framework):
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
def test_is_suppressed(install_framework):
    infinity_commons.get_and_verify_plan(lambda p: p['status'] == 'COMPLETE')
    time.sleep(5)
    response = dcos.http.get(cassandra_api_url('state/properties/suppressed'))
    response.raise_for_status()
    assert response.text == "true"
