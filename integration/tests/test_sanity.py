import dcos
import json
import pytest
import shakedown
from tests.command import (
    cassandra_api_url,
    check_health,
    uninstall
)
from tests.defaults import DEFAULT_NODE_COUNT, PACKAGE_NAME


@pytest.yield_fixture
def install_framework():
    uninstall()
    shakedown.install_package_and_wait(PACKAGE_NAME)
    check_health()
    yield

    uninstall()


@pytest.mark.sanity
def test_connect(install_framework):
    # TODO: remove fallback when universe has recent build with '/connection'
    try:
        result = dcos.http.get(cassandra_api_url('connection'))
    except:
        result = dcos.http.get(cassandra_api_url('connect'))

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
        result = dcos.http.get(cassandra_api_url('connect/address'))

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
        result = dcos.http.get(cassandra_api_url('connect/dns'))

    try:
        body = result.json()
        assert len(body) == DEFAULT_NODE_COUNT
    except:
        print('Failed to parse connect response')
        raise
