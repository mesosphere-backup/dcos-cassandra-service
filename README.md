# DCOS Cassandra Service

Running managed Cassandra rings on DCOS.

[Please click here for developer guide](dev-guide.md).

## Overview

### Benefits
DCOS Cassandra Service offers the following benefits:

* Easy installation
* Multiple Cassandra clusters
* Integrated monitoring

### Features
DCOS Cassandra Service provides the following features:

- Single command installation for rapid provisioning
- Persistent Storage volumes for enhanced data durability
- Add nodes for expanding capacity
- Health checks for monitoring
- HTTP API for programatic management

## Quick Start

- Step 1. Install [dcos-cli](https://github.com/mesosphere/dcos-cli).

- Step 2. Install a Cassandra cluster.

```bash
$ dcos package install cassandra
```

## Installation and Customization

### Default install configuration

To start a basic test cluster with 3 nodes and 2 seed nodes, run the following command with dcos-cli:

``` bash
$ dcos package install cassandra
```

### Custom install configuration
**TODO**

### Uninstall

Uninstalling a cluster is also straightforward. Replace app-id `cassandra` with the name of the cassandra instance to be uninstalled.

``` bash
$ dcos package uninstall --app-id=cassandra cassandra
```

The instance will still be present in zookeeper at `/[name]`, e.g., `/cassandra`. To completely clear the configuration, the zookeeper node must be removed.

### Changing configuration in flight
**TODO**

## Configuration Options

The following describes commonly used features of the DCOS Cassandra Service and how to configure them. View the [default `config.json` in DCOS Universe](https://github.com/mesosphere/universe/tree/version-2.x/repo/packages/C/cassandra) to see an enumeration of all possible options.

### Name

The name of this Cassandra instance in DCOS. This is the only option that cannot be changed once the Cassandra cluster is started; it can only be configured via the `dcos-cli --options` flag when first creating the Cassandra instance.

- **In dcos-cli options.json**: `name` = string (default: `cassandra`)
- **In Marathon**: The name cannot be changed after the cluster has started.

### Node Count

Configure the number of running nodes in a given Cassandra cluster. The default count at installation is three nodes.

- **In dcos-cli options.json**: `nodes` = integer (default: `3`)
- **In Marathon**: `NODES` = integer

### Seed Node Count

Configure the number of seed node in a given Cassandra cluster. The default count at installation is two seed nodes.

- **In dcos-cli options.json**: `seed-nodes` = integer (default: `2`)
- **In Marathon**: `SEED_NODES` = integer

## Connecting Clients

## Handling Errors

## Backup

Cassandra framework supports taking complete snapshot of the ring and uploading the artifacts to S3. To initiate the backup, the user needs to do following:

### CLI

Invoke backup using DCOS CLI:

```
dcos cassandra --framework-name=<framework-name> backup start --name=<backup-name> --external_location=s3://<bucket-name> --s3_access_key=<s3-access-key> --s3_secret_key=<s3-secret-key>
```

To check status of the backup:

```
dcos cassandra --framework-name=thecluster backup status
```

### API

First, create the request payload, for example in a file `backup.json`:
```
{
"name":"<backup-name>",
"external-location":"s3://<bucket-name>",
"s3-access-key":"<s3-access-key>",
"s3-secret-key":"<s3-secret-key>"
}
```

Then submit the request payload via `PUT` request to `/v1/backup/start`
```
curl -X PUT -H 'Content-Type: application/json' -d @backup.json http://cassandra.marathon.mesos:9000/v1/backup/start
{"status":"started", message:""}
```

To check status of the backup:
```
curl -X GET http://cassandra.marathon.mesos:9000/v1/backup/status
```

## Restore

Cassandra framework supports restore of snapshots on a new Cassandra ring. To restore a snapshot, use needs to do following:

### CLI

Invoke backup using DCOS CLI:

```
dcos cassandra --framework-name=<framework-name> restore start --name=<backup-name> --external_location=s3://<bucket-name> --s3_access_key=<s3-access-key> --s3_secret_key=<s3-secret-key>
```

To check status of the restore:

```
dcos cassandra --framework-name=thecluster restore status
```

### API

First, bring up a new instance of Cassandra cluster, with the same number of nodes as the cluster whose snapshot backup we are trying to restore.
Next, create the request payload, for example in a file `restore.json`:
```
{
"name":"<backup-name-to-restore>",
"external-location":"s3://<bucket-name-where-backups-are-stored>",
"s3-access-key":"<s3-access-key>",
"s3-secret-key":"<s3-secret-key>"
}
```

Next, submit the request payload via `PUT` request to `/v1/restore/start`
```
curl -X PUT -H 'Content-Type: application/json' -d @restore.json http://cassandra.marathon.mesos:9000/v1/restore/start
{"status":"started", message:""}
```

To check status of the restore:
```
curl -X GET http://cassandra.marathon.mesos:9000/v1/restore/status
```

## APIs

## Limitations

### Backup and Restore
1. Cassandra framework current takes a ring-wide snapshot serially in a rolling fashion to minimize the impact on the running cassandra cluster, as taking snapshot on a live-cluster can be a CPU intensive operation.
2. Cassandra framework also uploads and downloads backups serially in a rolling fashion to minimize the impact on link capacity. This limitation can be addressed in future, as we make DCOS and frameworks more topology aware.

## Extras - Demo with sample data
1. Install cassandra using:
   ```
   $ dcos package install cassandra
   ```
   
2. Login to an agent inside DCOS cluster running cassandra node, and then run following to launch a docker container:
   ```
   $ docker run --net=host -it mohitsoni/alpine-cqlsh:2.2.5 /bin/sh
   ```
   
3. Run ```/tmp/create.sh``` from inside the docker container to create a ```demo``` keyspace with a ```map``` table.
   ```
   $ /tmp/create.sh
   ```
   
4. Run ```cqlsh -f /tmp/insert.cql``` from inside the docker container to insert Fortune 1000 compnies inside the table.
   ```
   $ cqlsh -f /tmp/insert.cql
   ```
   
5. Run cqlsh, and execute following query to show the data.
   ```
   USE demo; SELECT * from demo.map;
   ```
