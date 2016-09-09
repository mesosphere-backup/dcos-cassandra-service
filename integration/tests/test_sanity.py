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

    return True
