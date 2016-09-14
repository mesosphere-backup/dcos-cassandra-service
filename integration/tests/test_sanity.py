import json
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


@pytest.yield_fixture
def install_framework():
    uninstall()
    shakedown.install_package_and_wait(PACKAGE_NAME)
    check_health()

    yield

    uninstall()


@pytest.mark.sanity
def test_connect(install_framework):
    result = requests.get(cassandra_api_url('connection'), headers=request_headers())

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
    result = requests.get(cassandra_api_url('connection/address'), headers=request_headers())

    try:
        body = result.json()
        assert len(body) == DEFAULT_NODE_COUNT
    except:
        print('Failed to parse connect response')
        raise


@pytest.mark.sanity
def test_connect_dns(install_framework):
    result = requests.get(cassandra_api_url('connection/dns'), headers=request_headers())

    try:
        body = result.json()
        assert len(body) == DEFAULT_NODE_COUNT
    except:
        print('Failed to parse connect response')
        raise
