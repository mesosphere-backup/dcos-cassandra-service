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
    get_dcos_command,
    get_cassandra_command,
    install,
    spin,
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


def get_first(collection, pred):
    for x in collection:
        if pred(x):
            return x

    return None


@pytest.mark.sanity
def test_connect():
    result = dcos.http.get(cassandra_api_url('connection'))
    body = result.json()
    assert len(body) == 3
    assert len(body["address"]) == DEFAULT_NODE_COUNT
    assert len(body["dns"]) == DEFAULT_NODE_COUNT
    assert body["vip"] == 'node.{}.l4lb.thisdcos.directory:9042'.format(PACKAGE_NAME)


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
    completed_plan = infinity_commons.get_and_verify_plan(
        lambda p: p['status'] == infinity_commons.PlanState.COMPLETE.value
    )
    assert completed_plan['status'] == infinity_commons.PlanState.COMPLETE.value


@pytest.mark.sanity
def test_env_unchanged():
    completed_plan = infinity_commons.get_and_verify_plan(
        lambda p: p['status'] == infinity_commons.PlanState.COMPLETE.value
    )
    assert completed_plan['status'] == infinity_commons.PlanState.COMPLETE.value

    mc = dcos.marathon.create_client()
    app = mc.get_app('/cassandra')
    app = infinity_commons.strip_meta(app)
    mc.update_app(app_id='/cassandra', payload=app)
    completed_plan = infinity_commons.get_and_verify_plan(
        lambda p: p['status'] == infinity_commons.PlanState.COMPLETE.value
    )
    assert completed_plan['status'] == infinity_commons.PlanState.COMPLETE.value


@pytest.mark.sanity
def test_nodes_increase_by_one():
    completed_plan = infinity_commons.get_and_verify_plan(
        lambda p: p['status'] == infinity_commons.PlanState.COMPLETE.value
    )
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
    plan = infinity_commons.get_and_verify_plan(
        lambda p: (
            p['status'] == infinity_commons.PlanState.COMPLETE.value and
            len(infinity_commons.filter_phase(p, "Deploy")['steps']) == 4 and
            (
                infinity_commons.filter_phase(p, "Deploy")['steps'][env_node_count - 1]['status'] ==
                infinity_commons.PlanState.COMPLETE.value
            )
        )
    )
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
                   len(infinity_commons.filter_phase(p, "Deploy")['steps']) == 3))
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
        (len(infinity_commons.filter_phase(p, "Deploy")['steps']) == 3))
    print(plan)
    assert plan['status'] == infinity_commons.PlanState.COMPLETE.value


@pytest.mark.sanity
def test_change_disk_should_fail():
    completed_plan = infinity_commons.get_and_verify_plan(
        lambda p: p['status'] == infinity_commons.PlanState.COMPLETE.value
    )
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
    plan = infinity_commons.get_and_verify_plan(
        lambda p: p['status'] == infinity_commons.PlanState.ERROR.value
    )
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
    plan = infinity_commons.get_and_verify_plan(
        lambda p: p['status'] == infinity_commons.PlanState.COMPLETE.value
    )
    print(plan)
    assert plan['status'] == infinity_commons.PlanState.COMPLETE.value


@pytest.mark.sanity
def test_cpus_increase_slightly():
    completed_plan = infinity_commons.get_and_verify_plan(
        lambda p: p['status'] == infinity_commons.PlanState.COMPLETE.value
    )
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
    plan = infinity_commons.get_and_verify_plan(
        lambda p: (
            p['status'] == infinity_commons.PlanState.COMPLETE.value and
            len(infinity_commons.filter_phase(p, "Deploy")['steps']) == 3
        )
    )
    print(plan)
    assert plan['status'] == infinity_commons.PlanState.COMPLETE.value


@pytest.mark.sanity
def test_is_suppressed():
    infinity_commons.get_and_verify_plan(lambda p: p['status'] == 'COMPLETE')
    time.sleep(5)
    response = dcos.http.get(cassandra_api_url('state/properties/suppressed'))
    response.raise_for_status()
    assert response.text == "true"


@pytest.mark.sanity
def test_node_is_replaced():
    infinity_commons.get_and_verify_plan(lambda p: p['status'] == 'COMPLETE')
    replaced_node_host = [
        t['slave_id'] for t in
        get_dcos_command('task --json') if t['name'] == 'node-0'
    ][0]
    replaced_node_task_id = get_cassandra_command('node replace node-0')[0]
    assert 'node-0' in replaced_node_task_id

    plan = infinity_commons.get_and_verify_plan(
        lambda p: (
            p['status'] == infinity_commons.PlanState.COMPLETE.value and
            len(infinity_commons.filter_phase(p, "Deploy")['steps']) == 3
        )
    )
    print(plan)
    assert plan['status'] == infinity_commons.PlanState.COMPLETE.value

    # Check that we've created a new task with a new id, waiting until a new one comes up.
    def get_status():
        return get_first(
            get_dcos_command('task --json'), lambda t: t['name'] == 'node-0'
        )['id']

    def success_predicate(task_id):
        return task_id != replaced_node_task_id, 'Task ID for replaced node did not change'

    spin(get_status, success_predicate)

    # Check cluster status with nodetool to assure that the new node has rejoined the cluster
    # and the old node has been removed, waiting until it's running (status "UN").
    def get_status():
        node1_host = get_first(
            get_first(get_dcos_command('task --json'), lambda t: t['name'] == 'node-1')['labels'],
            lambda label: label['key'] == 'offer_hostname'
        )['value']

        return shakedown.run_command_on_agent(
            node1_host,
            "docker run -t --net=host pitrho/cassandra-nodetool nodetool -p 7199 status"
        )

    def success_predicate(status):
        command_succeeded, status = status
        succeeded = (
            command_succeeded and
            len([x for x in status.split('\n') if x.startswith('UN')]) == DEFAULT_NODE_COUNT
        )

        return succeeded, 'Node did not successfully rejoin cluster'

    spin(get_status, success_predicate)
