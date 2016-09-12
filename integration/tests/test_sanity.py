import json
import dcos
import pytest
import requests
import shakedown

from tests.command import (
    cassandra_api_url,
    check_health,
    get_cassandra_config,
    marathon_api_url,
    request,
    spin,
    uninstall
)
from tests.defaults import DEFAULT_NODE_COUNT, PACKAGE_NAME, request_headers
from . import infinity_commons


@pytest.yield_fixture
def install_framework():
    try:
        shakedown.install_package_and_wait(PACKAGE_NAME)
    except dcos.errors.DCOSException:
        uninstall()
        shakedown.install_package_and_wait(PACKAGE_NAME)

    check_health()

    yield

    uninstall()


def test_connect(install_framework):
    result = requests.get(cassandra_api_url('connection'), headers=request_headers())

    try:
        body = result.json()
        assert len(body) == 2
        assert len(body["address"]) == DEFAULT_NODE_COUNT
        assert len(body["dns"]) == DEFAULT_NODE_COUNT
    except:
        print('Failed to parse connect response')
        return False


def test_connect_address(install_framework):
    result = requests.get(cassandra_api_url('connection/address'), headers=request_headers())

    try:
        body = result.json()
        assert len(body) == DEFAULT_NODE_COUNT
    except:
        print('Failed to parse connect response')
        return False


def test_connect_dns(install_framework):
    result = requests.get(cassandra_api_url('connection/dns'), headers=request_headers())

    try:
        body = result.json()
        assert len(body) == DEFAULT_NODE_COUNT
    except:
        print('Failed to parse connect response')
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
    completed_plan = infinity_commons.get_and_verify_plan(lambda p: p['status'] == infinity_commons.PlanState.COMPLETE.value)
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
    plan = infinity_commons.get_and_verify_plan(lambda p: p['status'] == infinity_commons.PlanState.ERROR.value and len(infinity_commons.filter_phase(p, "Deploy")['blocks']) == 3) 
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
    plan = infinity_commons.get_and_verify_plan(lambda p: p['status'] == infinity_commons.PlanState.COMPLETE.value and len(infinity_commons.filter_phase(p, "Deploy")['blocks']) == 3) 
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

    