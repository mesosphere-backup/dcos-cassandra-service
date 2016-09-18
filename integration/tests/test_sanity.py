import json
import pytest

import dcos
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


@pytest.mark.sanity
def test_is_suppressed():
    response = dcos.http.get(cassandr_api_url('service/kafka/v1/state/properties/suppressed'))
    response.raise_for_status()
    assert response.text == "true"
