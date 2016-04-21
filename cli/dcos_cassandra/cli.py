#
#    Copyright (C) 2015 Mesosphere, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""DCOS Cassandra"""

import click
import pkg_resources
from dcos_cassandra import cassandra_utils as cu
from dcos_cassandra import (backup_api, cleanup_api, nodes_api, repair_api,
                            restore_api, seeds_api)


@click.group()
def cli():
    pass


@cli.group(invoke_without_command=True)
@click.option('--info/--no-info', default=False)
@click.option('--name', help='Name of the Cassandra instance to query.')
@click.option('--config-schema',
              help='Prints the config schema for Cassandra.',
              is_flag=True)
def cassandra(info, name, config_schema):
    if info:
        print("Deploy and manage Cassandra clusters")
    if name:
        cu.set_fwk_name(name)
    if config_schema:
        print_schema()


def print_schema():
    schema = pkg_resources.resource_string(
        'dcos_cassandra',
        'data/config-schema/cassandra.json'
    ).decode('utf-8')
    print(schema)


@cassandra.group()
@click.option('--name', help='Name of the cassandra cluster to query')
def node(name):
    """Manage Cassandra nodes"""
    if name:
        cu.set_fwk_name(name)


@cassandra.command()
@click.option('--name', help='Name of the cassandra cluster to query')
def seeds(name):
    """Retrieve seed node information"""
    if name:
        cu.set_fwk_name(name)
    cu.print_json(seeds_api.seeds())


@cassandra.group()
@click.option('--name', help='Name of the cassandra cluster to query')
def backup(name):
    """Backup Cassandra data"""
    if name:
        cu.set_fwk_name(name)


@cassandra.group()
@click.option('--name', help='Name of the cassandra cluster to query')
def restore(name):
    """Restore Cassandra cluster from backup"""
    if name:
        cu.set_fwk_name(name)


@cassandra.group()
@click.option('--name', help='Name of the cassandra cluster to query')
def cleanup(name):
    """Cleanup old token mappings"""
    if name:
        cu.set_fwk_name(name)


@cassandra.group()
@click.option('--name', help='Name of the cassandra cluster to query')
def repair(name):
    """Perform primary range repair."""
    if name:
        cu.set_fwk_name(name)


@node.command()
def list():
    """Lists all nodes"""
    cu.print_json(nodes_api.list())


@cassandra.command()
@click.option('--name', help='Name of the cassandra cluster to query')
@click.option('--address',
              is_flag=True,
              help='If set the addresses of the nodes are returned')
@click.option('--dns',
              is_flag=True,
              help='If set the dns names of the nodes are returned')
def connection(name, address, dns):
    """Provides connection information"""
    if name:
        cu.set_fwk_name(name)

    if (address and dns) or (not address and not dns):
        response = nodes_api.connection()
    elif address:
        response = nodes_api.connection_address()
    else:
        response = nodes_api.connection_dns()
    cu.print_json(response)


@node.command()
@click.argument("node_id")
def describe(node_id):
    """Describes a single node"""
    cu.print_json(nodes_api.describe(node_id))


@node.command()
@click.argument("node_id")
def status(node_id):
    """Gets the status of a single node."""
    cu.print_json(nodes_api.status(node_id))


@node.command()
@click.argument("node_id")
def restart(node_id):
    """Restarts a single node job"""
    success = nodes_api.restart(node_id)
    if not success:
        print("Error occured while restarting node: {}".format((str(node_id))))
    else:
        print("Successfully submitted request for restarting node: {}".format(
                str(node_id)))


@node.command()
@click.argument("node_id")
def replace(node_id):
    """Replace a single node job"""
    success = nodes_api.replace(node_id)
    if not success:
        print("Error occured while replacing node: {}".format((str(node_id))))
    else:
        print("Successfully submitted request to replace node: {}".format(
                str(node_id)))


@backup.command('start')
@click.option('--backup_name', help='Name of the snapshot')
@click.option('--external_location',
              help='External location where the snapshot should be stored.')
@click.option('--s3_access_key', help='S3 access key')
@click.option('--s3_secret_key', help='S3 secret key')
def backup_start(backup_name, external_location, s3_access_key, s3_secret_key):
    """Perform cluster backup via snapshot mechanism"""
    response = backup_api.start_backup(backup_name, external_location,
                                       s3_access_key,
                                       s3_secret_key)

    if response.status_code % 200 < 100:
        print("Successfully started backup. " +
              "Please check the backup status using the status sub-command.")
    else:
        cu.print_json(response)


@backup.command('status')
def backup_status():
    """Displays the status of the restore"""
    cu.print_json(backup_api.status())


@restore.command('start')
@click.option('--backup_name', help='Name of the snapshot to restore')
@click.option('--external_location',
              help='External location where the snapshot is stored.')
@click.option('--s3_access_key', help='S3 access key')
@click.option('--s3_secret_key', help='S3 secret key')
def restore_start(backup_name, external_location,
                  s3_access_key, s3_secret_key):
    """Restores cluster to a snapshot"""
    response = restore_api.start_restore(backup_name, external_location,
                                         s3_access_key, s3_secret_key)

    if response.status_code % 200 < 100:
        print("Successfully started restore. " +
              "Please check the restore status using the status sub-command.")
    else:
        cu.print_json(response)


@restore.command('status')
def restore_status():
    """Displays the status of the restore"""
    cu.print_json(restore_api.status())


@cleanup.command('start')
@click.option('--nodes',
              help='A list of the nodes to cleanup or * for all.',
              default='*')
@click.option('--key_spaces', help='The key spaces to cleanup. Empty for all',
              default=None)
@click.option('--column_families', help='The column families to cleanup.',
              default=None)
def cleanup_start(nodes, key_spaces, column_families):
    """Perform cluster cleanup of deleted or moved keys"""
    if nodes == '*':
        node_ids = ['*']
    else:
        node_ids = []
        for nid in nodes.split(','):
            node_ids.append("node-{}".format(nid))
    response = cleanup_api.start_cleanup(node_ids, key_spaces, column_families)
    if response.status_code % 200 < 100:
        print("Successfully started cleanup")
    else:
        cu.print_json(response)


@repair.command('start')
@click.option('--nodes',
              help='A list of the nodes to repair or * for all.',
              default='*')
@click.option('--key_spaces', help='The key spaces to repair. Empty for all',
              default=None)
@click.option('--column_families', help='The column families to repair.',
              default=None)
def repair_start(nodes, key_spaces, column_families):
    """Perform primary range anti-entropy repair"""
    if nodes == '*':
        node_ids = ['*']
    else:
        node_ids = []
        for nid in nodes.split(','):
            node_ids.append("node-{}".format(nid))
    response = repair_api.start_repair(node_ids, key_spaces, column_families)
    if response.status_code % 200 < 100:
        print("Successfully started repair")
    else:
        cu.print_json(response)


def main():
    cli(obj={})


if __name__ == "__main__":
    main()
