import dcos.http
import pytest
import shakedown


from . import infinity_commons

from tests.command import (
    check_health,
    get_cassandra_command,
    get_dcos_command,
    install,
    marathon_api_url_with_param,
    request,
    spin,
    uninstall,
)

from tests.defaults import DEFAULT_NODE_COUNT, PACKAGE_NAME

def setup_module(module):
    uninstall()


def teardown_module(module):
    uninstall()


@pytest.mark.special
def test_upgrade_downgrade():
    # Ensure both Universe and the test repo exist.
    if len(shakedown.get_package_repos()['repositories']) != 2:
        print('No cassandra test repo found.  Skipping test_upgrade_downgrade')
        return

    test_repo_name, test_repo_url = get_test_repo_info()
    test_version = get_pkg_version()
    print('Found test version: {}'.format(test_version))
    remove_repo(test_repo_name, test_version)
    master_version = get_pkg_version()
    print('Found master version: {}'.format(master_version))

    print('Installing master version')
    install(package_version = master_version)
    check_health()
    plan = infinity_commons.get_and_verify_plan(lambda p: p['status'] == infinity_commons.PlanState.COMPLETE.value)
    assert plan['status'] == infinity_commons.PlanState.COMPLETE.value

    # TODO: write some data

    print('Upgrading to test version')
    destroy_service()
    add_repo(test_repo_name, test_repo_url, master_version)
    install(package_version = test_version)
    check_post_version_change_health()

    print('Downgrading to master version')
    destroy_service()
    install(package_version = master_version)
    check_post_version_change_health()


def get_test_repo_info():
    repos = shakedown.get_package_repos()
    test_repo = repos['repositories'][0]
    return test_repo['name'], test_repo['uri']


def get_pkg_version():
    cmd = 'package describe {}'.format(PACKAGE_NAME)
    print("get_pkg_version cmd: " + cmd)
    pkg_description = get_dcos_command(cmd)
    print("pkg_description: " + pkg_description)
    return pkg_description['version']


def remove_repo(repo_name, prev_version):
    assert shakedown.remove_package_repo(repo_name)
    new_default_version_available(prev_version)


def add_repo(repo_name, repo_url, prev_version):
    assert shakedown.add_package_repo(
        repo_name,
        repo_url,
        0)
    # Make sure the new repo packages are available
    new_default_version_available(prev_version)


def new_default_version_available(prev_version):
    def fn():
        get_pkg_version()
    def success_predicate(pkg_version):
        return (pkg_version != prev_version, 'Package version has not changed')
    spin(fn, success_predicate)


def destroy_service():
    destroy_endpoint = marathon_api_url_with_param('apps', PACKAGE_NAME)
    request(dcos.http.delete, destroy_endpoint)
    # Make sure the scheduler has been destroyed
    def fn():
        shakedown.get_service(PACKAGE_NAME)

    def success_predicate(service):
        return (service == None, 'Service not destroyed')

    spin(fn, success_predicate)


def check_post_version_change_health():
    check_health()
    check_scheduler_health()
    # TODO: verify data integrity


def check_scheduler_health():
    # Make sure scheduler endpoint is responding and all nodes are available
    def fn():
        try:
            return get_cassandra_command('node list')
        except RuntimeError:
            return []

    def success_predicate(brokers):
        return (len(brokers) == DEFAULT_NODE_COUNT,
                'Scheduler and all nodes not available')

    spin(fn, success_predicate)
