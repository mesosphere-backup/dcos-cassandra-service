DCOS Cassandra Service Guide
======================

* [DCOS Cassandra Service Guide](#dcos-cassandra-service-guide)
    * [Overview](#overview)
      * [Benefits](#benefits)
      * [Features](#features)
      * [Related Services](#related-services)
    * [Getting Started](#getting-started)
      * [Quick Start](#quick-start)
      * [Install and Customize](#install-and-customize)
        * [Default Installation] (#default-installation)
        * [Custom Installation](#custom-installation)
        * [Minimal Installation] (#minimal-installation)
      * [Multiple Cassandra Cluster Installation](#multiple-cassandra-cluster-installation)
        * [Installation Plan](#installation-plan)
          * [Viewing the Installation Plan](#viewing-the-installation-plan)
          * [Plan Errors](#plan-errors)
          * [Reconciliation Phase](#reconciliation-phase)
          * [Deploy Phase](#deploy-phase)
          * [Pausing Installation](#pausing-installation)
          * [Resuming Installation](#resuming-installation)
      * [Uninstall](#uninstall)
    * [Configuring](#configuring)
      * [Changing Configuration at Runtime](#changing-configuration-at-runtime)
        * [Configuration Deployment Strategy](#configuration-deployment-strategy)
          * [Configuration Update Plans](#configuration-update-plans)
      * [Configuration Update](#configuration-update)
      * [Configuration Options](#configuration-options)
        * [Service Configuration](#service-configuration)
        * [Node Configuration](#node-configuration)
        * [Cassandra Application Configuration](#cassandra-application-configuration)
        * [Communication Configuration](#communication-configuration)
        * [Commit Log Configuration](#commit-log-configuration)
        * [Column Index Configuration](#column-index-configuration)
        * [Hinted Handoff Configuration](#hinted-handoff-configuration)
        * [Dynamic Snitch Configuration](#dynamic-snitch-configuration)
        * [Global Key Cache Configuration](#global-key-cache-configuration)
        * [Global Row Cache Configuration](#global-row-cache-configuration)
      * [Operating System Configuration](#operating-system-configuration)
   * [Connecting Clients](#connecting-clients)
     * [Connection Info Using the CLI](#connection-info-using-the-cli)
     * [Connection Info Response](#connection-info-response)
     * [Configuring the CQL Driver](#configuring-the-cql-driver)
       * [Adding the Driver to Your Application](#adding-the-driver-to-your-application)
       * [Connecting the CQL Driver\.](#connecting-the-cql-driver)
    * [Managing](#managing)
      * [Add a Node](#add-a-node)
      * [Node Status] (#node-status)
      * [Node Info] (#node-info)
      * [Cleanup](#cleanup)
      * [Repair] (#repair)
      * [Backup and Restore](#backup-and-restore)
        * [Backup](#backup)
        * [Restore](#restore)
    * [Troubleshooting](#troubleshooting)
    * [API Reference](#api-reference)
      * [Configuration](#configuration-api])
        * [View the Installation Plan](#view-the-installation-plan)
        * [Retrieve Connection Info](#retrieve-connection-info)
        * [Pause Installation](#pause-installation)
        * [Resume Installation](#resume-installation)
      * [Managing](#managing)
        * [Node Status] (#node-status)
        * [Node Info] (#node-info)
        * [Cleanup](#cleanup)
        * [Repair] (#repair)
        * [Backup] (#backup)
        * [Restore](#restore)
    * [Limitations](#limitations)
    * [Development](#development)

## Overview

DCOS Cassandra is an automated service that makes it easy to deploy and manage on Mesosphere DCOS. DCOS Cassandra eliminates nearly all of the complexity traditional associated with managing a Cassandra cluster. Apache Cassandra is distributed database management system designed to handle large amounts of data across many nodes, providing horizonal scalablity and high availability with no single point of failure, with a simple query language (CQL). For more information on Apache Cassandra, see the Apache Cassandra [documentation] (http://docs.datastax.com/en/cassandra/2.2/pdf/cassandra22.pdf). DCOS Cassandra gives you direct access to the Cassandra API so that existing applications can interoperate. You can configure and install DCOS Cassandra in moments. Multiple Cassandra clusters can be installed on DCOS and managed independently, so you can offer Cassandra as a managed service to your organization.

### Benefits

DCOS Cassandra offers the following benefits:

- Easy installation
- Multiple Cassandra clusters
- Elastic cluster scaling
- Replication for high availability
- Integrated monitoring


### Features

DCOS Cassandra provides the following features:

- Single command installation for rapid provisioning
- Persistent storage volumes for enhanced data durability
- Runtime configuration and software updates for high availability
- Health checks and metrics for monitoring
- Backup and restore for disaster recovery
- Cluster wide automation Cleanup and Repair 

### Related Services

- [DCOS Spark](https://docs.mesosphere.com/manage-service/spark)

## Getting Started

### Quick Start

Let's get started quickly by installing Cassandra cluster using DCOS CLI.

**Note:** Your cluster must have at least 3 private nodes.

```bash
$ dcos package install cassandra
```

Once your cluster is installed. We'll retrieve connection information by running `connection` command:

```bash
$ dcos cassandra connection
{
    "nodes": [
        "10.0.2.136:9042",
        "10.0.2.138:9042",
        "10.0.2.137:9042"
    ]
}
```

Let's [SSH into a DC/OS node](https://docs.mesosphere.com/administration/sshcluster/):

```
$ dcos node ssh --master-proxy --leader
core@ip-10-0-6-153 ~ $ 
```

We are now inside our DC/OS cluster and can connect to our Cassandra cluster directly. Let's launch a docker container containing `cqlsh` to connect to our cassandra cluster. We'll use one of the hosts that we retrieved from the `connection` command that we ran previously:

```
core@ip-10-0-6-153 ~ $ docker run -ti cassandra:2.2.5 cqlsh 10.0.2.136
cqlsh>
```

We are now connected to our Cassandra cluster. Let's create a sample keyspace called `demo`:

```
cqlsh> CREATE KEYSPACE demo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
```

Next, let's create a sample table called `map` in our `demo` keyspace:

```
cqlsh> USE demo;CREATE TABLE map (key varchar, value varchar, PRIMARY KEY(key));
```

Let's insert some data in our table:

```
cqlsh> INSERT INTO demo.map(key, value) VALUES('Cassandra', 'Rocks!');
cqlsh> INSERT INTO demo.map(key, value) VALUES('StaticInfrastructure', 'BeGone!');
cqlsh> INSERT INTO demo.map(key, value) VALUES('Buzz', 'DC/OS is the new black!');
```    

Now we have inserted some data, let's query it back to make sure it's persisted correctly:

```
cqlsh> SELECT * FROM demo.map;
```    

### Install and Customize

#### Default Installation
Prior to installing a default cluster, ensure that your DCOS cluster has at least 3 DCOS slaves with 8 Gb of memory, 10 Gb of disk available on each agent. Also, ensure that ports 7000,7001,7199,9042, and 9160 are available.
To start a the default cluster, run the following command on the DCOS CLI. The default installation may not be sufficient for a production deployment, but all cluster operations will work. If you are planning a production deployment with 3 replicas of each value and with local quorum consistency for read and write operations (a very common use case), this configuration is sufficient for development and testing purposes, and it may be scaled to a production deployment.

``` bash
$ dcos package install cassandra
```

This command creates a new Cassandra cluster with 3 nodes. Two clusters cannot share the same name, so installing additional clusters beyond the default cluster requires [customizing the `framework-name` at install time](#custom-installation) for each additional instance.

All `dcos cassandra` CLI commands have a `--framework-name` argument that allows the user to specify which Cassandra instance to query. If you do not specify a framework name, the CLI assumes the default value, `cassandra`. The default value for `--framework-name` can be customized via the DCOS CLI configuration.

``` bash
$ dcos config set cassandra.framework_name new_default_name
```

#### Minimal Installation
You may wish to install Cassandra on a local DCOS cluster. For this, you can use [dcos-vagrant](https://github.com/mesosphere/dcos-vagrant).
As with the default installation, you must ensure that ports 7000,7001,7199,9042, and 9160 are available. Note that this configuration will not support replication of any kind, but it may be sufficient for early stage evaluation and development. 

To start a minimal cluster with a single node, create a JSON options file named `sample-cassandra-minimal.json`:

``` json
{
    "nodes": {
        "cpus": 0.5,
        "mem": 2048,
        "disk": 4096,
        "heap": {
            "size": 1024,
            "new": 100
        },
        "volume_size": 4096,
        "count": 1,
        "seeds": 1
    }
}
```
This will create a single node cluster with 2 Gb of memory and 4Gb of disk. Note that you will need an addition 3 Gb of memory for the DCOS Cassandra Framework itself. Also, cluster operations will require an addition 512 Mb of memory.

#### Custom Installation
If you are ready to ship into production, you will likely need to customize the deployment to suite the workload requirements of application(s). You can customize the default deployment by creating a JSON file. Then pass it to `dcos package install` using the `--options` parameter.

Sample JSON options file named `sample-cassandra.json`:

``` json
{
    "node": {
        "nodes": 10,
        "seeds": 3
    }
}
```

The command below creates a cluster using `sample-cassandra.json`:

``` bash
$ dcos package install --options=sample-cassandra.json cassandra
```

This cluster will have 10 nodes and 3 seeds instead of the default values of 3 nodes and 2 seeds.
See [Configuration Options](#configuration-options) for a list of fields that can be customized via an options JSON file when the Cassandra cluster is created.

### Multiple Cassandra Cluster Installation

Installing multiple Cassandra clusters is identical to installing a Cassandra cluster with a custom configuration as described above. Use a JSON options file to specify a unique `framework-name` for each installation:

``` json
$ cat cassandra1.json
{
   "service": {
       "name": "cassandra1"
   }
}

$ dcos package install cassandra --options=cassandra1.json
```

In order to avoid port conflicts, by default you cannot collocate more than one Cassandra instance on the same node. 

#### Installation Plan
When the DCOS Cassandra service is initially installed it will generate an installation plan as shown below. 

``` json
{
    "errors": [], 
    "phases": [
        {
            "blocks": [
                {
                    "has_decision_point": false, 
                    "id": "738122a7-8b52-4d45-a2b0-41f625f04f87", 
                    "message": "Reconciliation complete", 
                    "name": "Reconciliation", 
                    "status": "Complete"
                }
            ], 
            "id": "0836a986-835a-4811-afea-b6cb9ddcd929", 
            "name": "Reconciliation", 
            "status": "Complete"
        }, 
        {
            "blocks": [
                {
                    "has_decision_point": false, 
                    "id": "440485ec-eba2-48a3-9237-b0989dbe9f68", 
                    "message": "Deploying Cassandra node node-0", 
                    "name": "node-0", 
                    "status": "Complete"
                }, 
                {
                    "has_decision_point": false, 
                    "id": "84251eb9-218c-4700-a03c-50018b90d5a8", 
                    "message": "Deploying Cassandra node node-1", 
                    "name": "node-1", 
                    "status": "InProgress"
                }, 
                {
                    "has_decision_point": false, 
                    "id": "aad765fe-5aa5-4d4e-bf66-abbb6a15e125", 
                    "message": "Deploying Cassandra node node-2", 
                    "name": "node-2", 
                    "status": "Pending"
                }
            ], 
            "id": "c4f61c72-038d-431c-af73-6a9787219233", 
            "name": "Deploy", 
            "status": "InProgress"
        }
    ], 
    "status": "InProgress"
}
```

##### Viewing the Installation Plan
The plan can be viewed from the API via the REST endpoint. A curl example is provided below.

``` bash
curl http:/<dcos_url>/service/cassandra/v1/plan
```

##### Plan Errors
If there are any errors that prevent installation, these errors are dispayed in the errors list. The presence of errors indicates that the installation cannot progress. See the [Troubleshooting](#troubleshooting) section for information on resolving errors.

##### Reconciliation Phase
The first phase of the installation plan is the reconciliation phase. This phase ensures that the DCOS Cassandra service maintains the correct status for the Cassandra nodes that it has deployed. Reconciliation is a normal operation of the DCOS Cassandra Service and occurs each time the service starts. See [the Mesos documentation](http://mesos.apache.org/documentation/latest/reconciliation) for more information.

##### Deploy Phase
The second phase of the installation is the deploy phase. This phase will deploy the requested number of Cassandra nodes. Each block in the phase represents an individual Cassandra node. In the plan shown above the first node, node-0, has been deployed, the second node, node-1, is in the process of being deployed, and the third node, node-2, is pending deployment based on the completion of node-1.

##### Pausing Installation
In order to pause installation, issue a REST API request as shown below. The installation will pause after completing installation of the current node and wait for user input.

``` bash
curl -X PUT http:/<dcos_url>/service/cassandra/v1/plan?cmd=interrupt
```

##### Resuming Installation
If the installation has been paused, the REST API request below will resume installation at the next pending node.

``` bash
curl -X PUT http://<dcos_url>/service/cassandra/v1/plan?cmd=proceed
```

### Uninstall

Uninstalling a cluster is straightforward. Replace `cassandra` with the name of the Cassandra instance to be uninstalled.

``` bash
$ dcos package uninstall --app-id=cassandra
```

Then, use the [framework cleaner script](https://github.com/mesosphere/framework-cleaner) to remove your Cassandra instance from Zookeeper and destroy all data associated with it. The arguments the script requires are derived from your framework name:

- `framework-role` is `<framework-name>-role`.
- `framework-principle` is `<framework-name>-principal`.
= `zk_path` is `<framework-name>`.

## Configuring

### Changing Configuration at Runtime

You can customize your cluster in-place when it is up and running.

The Cassandra scheduler runs as a Marathon process and can be reconfigured by changing values within Marathon. These are the general steps to follow:

1. View your Marathon dashboard at `http://<dcos_url>/marathon`
2. In the list of `Applications`, click the name of the Cassandra service to be updated.
3. Within the Cassandra instance details view, click the `Configuration` tab, then click the `Edit` button.
4. In the dialog that appears, expand the `Environment Variables` section and update any field(s) to their desired value(s). For example, to increase the number of nodes, edit the value for `NODES`. Click `Change and deploy configuration` to apply any changes and cleanly reload the Cassandra scheduler. The Cassandra cluster itself will persist across the change.

#### Configuration Deployment Strategy

Configuration updates are rolled out through execution of Update Plans. You can configure the way these plans are executed.

##### Configuration Update Plans 
This configuration update strategy is analogous to the installation procedure above. If the configuration update is accepted, there will be no errors in the generated plan, and a rolling restart will be performed on all nodes to apply the updated configuration. However, the default strategy can be overridden by a strategy the user provides.

### Configuration Update

Make the REST request below to view the current plan:

``` bash
curl -v http://<dcos_url>/service/cassandra/v1/plan
```

Response will look similar to this:

``` json
{
    "errors": [], 
    "phases": [
        {
            "blocks": [
                {
                    "has_decision_point": false, 
                    "id": "738122a7-8b52-4d45-a2b0-41f625f04f87", 
                    "message": "Reconciliation complete", 
                    "name": "Reconciliation", 
                    "status": "Complete"
                }
            ], 
            "id": "0836a986-835a-4811-afea-b6cb9ddcd929", 
            "name": "Reconciliation", 
            "status": "Complete"
        }, 
        {
            "blocks": [
                {
                    "has_decision_point": true, 
                    "id": "440485ec-eba2-48a3-9237-b0989dbe9f68", 
                    "message": "Deploying Cassandra node node-0", 
                    "name": "node-0", 
                    "status": "Pending"
                }, 
                {
                    "has_decision_point": true, 
                    "id": "84251eb9-218c-4700-a03c-50018b90d5a8", 
                    "message": "Deploying Cassandra node node-1", 
                    "name": "node-1", 
                    "status": "Pending"
                }, 
                {
                    "has_decision_point": false, 
                    "id": "aad765fe-5aa5-4d4e-bf66-abbb6a15e125", 
                    "message": "Deploying Cassandra node node-2", 
                    "name": "node-2", 
                    "status": "Pending"
                }
            ], 
            "id": "c4f61c72-038d-431c-af73-6a9787219233", 
            "name": "Deploy", 
            "status": "Pending"
        }
    ], 
    "status": "InProgress"
}
```

If you want to interrupt a configuration update that is in progress, enter the `interrupt` command.

If you query the plan again, the response will look like this (notice `status: "Waiting"`):

``` json
{
    "errors": [], 
    "phases": [
        {
            "blocks": [
                {
                    "has_decision_point": false, 
                    "id": "738122a7-8b52-4d45-a2b0-41f625f04f87", 
                    "message": "Reconciliation complete", 
                    "name": "Reconciliation", 
                    "status": "Complete"
                }
            ], 
            "id": "0836a986-835a-4811-afea-b6cb9ddcd929", 
            "name": "Reconciliation", 
            "status": "Complete"
        }, 
        {
            "blocks": [
                {
                    "has_decision_point": false, 
                    "id": "440485ec-eba2-48a3-9237-b0989dbe9f68", 
                    "message": "Deploying Cassandra node node-0", 
                    "name": "node-0", 
                    "status": "Complete"
                }, 
                {
                    "has_decision_point": false, 
                    "id": "84251eb9-218c-4700-a03c-50018b90d5a8", 
                    "message": "Deploying Cassandra node node-1", 
                    "name": "node-1", 
                    "status": "Pending"
                }, 
                {
                    "has_decision_point": false, 
                    "id": "aad765fe-5aa5-4d4e-bf66-abbb6a15e125", 
                    "message": "Deploying Cassandra node node-2", 
                    "name": "node-2", 
                    "status": "InProgress"
                }
            ], 
            "id": "c4f61c72-038d-431c-af73-6a9787219233", 
            "name": "Deploy", 
            "status": "Waiting"
        }
    ], 
    "status": "Waiting"
}
```

**Note:** The interrupt command can’t stop a block that is `InProgress`, but it will stop the change on the subsequent blocks.

Enter the `continue` command to resume the update process.

After you execute the continue operation, the plan will look like this:

``` json
{
    "errors": [], 
    "phases": [
        {
            "blocks": [
                {
                    "has_decision_point": false, 
                    "id": "738122a7-8b52-4d45-a2b0-41f625f04f87", 
                    "message": "Reconciliation complete", 
                    "name": "Reconciliation", 
                    "status": "Complete"
                }
            ], 
            "id": "0836a986-835a-4811-afea-b6cb9ddcd929", 
            "name": "Reconciliation", 
            "status": "Complete"
        }, 
        {
            "blocks": [
                {
                    "has_decision_point": false, 
                    "id": "440485ec-eba2-48a3-9237-b0989dbe9f68", 
                    "message": "Deploying Cassandra node node-0", 
                    "name": "node-0", 
                    "status": "Complete"
                }, 
                {
                    "has_decision_point": false, 
                    "id": "84251eb9-218c-4700-a03c-50018b90d5a8", 
                    "message": "Deploying Cassandra node node-1", 
                    "name": "node-1", 
                    "status": "Complete"
                }, 
                {
                    "has_decision_point": false, 
                    "id": "aad765fe-5aa5-4d4e-bf66-abbb6a15e125", 
                    "message": "Deploying Cassandra node node-2", 
                    "name": "node-2", 
                    "status": "Pending"
                }
            ], 
            "id": "c4f61c72-038d-431c-af73-6a9787219233", 
            "name": "Deploy", 
            "status": "Pending"
        }
    ], 
    "status": "InProgress"
}
```

### Configuration Options

The following describes the most commonly used features of DCOS Cassandra and how to configure them via the DCOS CLI and in Marathon. There are two methods of configuring a Cassandra cluster. The configuration may be specified using a JSON file during installation via the DCOS command line (See the [Installation section](#installation)) or via modification to the Service Scheduler’s Marathon environment at runtime (See the [Configuration Update section](#configuration-update)). Note that some configuration options may only be specified at installation time, but these generally relate only to the service’s registration and authentication with the DCOS scheduler.

#### Service Configuration

The service configuration object contains properties that MUST be specified during installation and CANNOT be modified after installation is in progress. This configuration object is similar across all DCOS Infinity services. Service configuration example:

``` json
{
    "service": {
        "name": "cassandra2",
        "role": "cassandra_role",
        "principal": "cassandra_principal",
        "secret" : "/path/to/secret_file"
    }
}
```

| Property | Type | Description         |
|----------| ------ | ----------------- |
| name     | string | The name of the Cassandra cluster. |
| role     | string | The authentication and resource role of the Cassandra cluster. |
| principal| string | The authentication principal for the Cassandra cluster. |
|secret    | string | An optional path to the file containing the secret that the service will use to authenticate with the Mesos Master in the DCOS cluster. This parameter is optional, and should be omitted unless the DCOS deployment is specifically configured for authentication. |

- **In the DCOS CLI, options.json**: `framework-name` = string (default: `cassandra`)
- **In Marathon**: The framework name cannot be changed after the cluster has started.

#### Node Configuration

The node configuration object corresponds to the configuration for Cassandra nodes in the Cassandra cluster. Node configuration MUST be specified during installation and MAY be modified during configuration updates. All of the properties except for `disk` MAY be modified during the configuration update process.

Example node configuration:

``` json
{
    "nodes": {
        "cpus": 0.5,
        "mem": 4096,
        "disk": 10240,
        "heap": {
            "size": 2048,
            "new": 400
        }
        "count": 3,
        "seeds": 2
    }
}
```

| Property    | Type    | Description     |    
| ----------- | ------- | --------------- |
| cpus        | number  | The number of cpu shares allocated to the container where the Cassandra process resides. Currently, due to a bug in Mesos, it is not safe to modify this parameter. |
| mem         | integer | The amount of memory, in MB, allocated to the container where the Cassandra process resides. This value MUST be larger than the specified max heap size. Make sure to allocate enough space for additional memory used by the JVM and other overhead. |
| disk        | integer | The amount of disk, in MB, allocated to a Cassandra node in the cluster. NOTE That once this value is configured it can not be changed|
| heap.size   | integer | The maximum and minimum heap size used by the Cassandra process in MB. This value SHOULD be at least 2 GB, and it SHOULD be no larger than 80% of the allocated memory for the container. Specifying very large heaps, greater than 8 GB, is currently not a supported configuration. |
| heap.new    | integer | The young generation heap size in MB. This value should be set at roughly 100MB per allocated CPU core. Increasing the size of this value will generally increase the length of garbage collection pauses. Smaller values will increase the frequency of garbage collection pauses. |
| count       | integer | The number of nodes in the Cassandra cluster. This value MUST be between 3 and 100. |
| seeds      | integer  | The number of seed nodes that the service will use to seed the cluster. The service selects seed nodes dynamically based on the current state of the cluster. 2 - 5 seed nodes is generally sufficient. |

#### Cassandra Application Configuration

The Cassandra application is configured via the Cassandra JSON object. **You should not modify these settings without strong reason and an advanced knowledge of Cassandra internals and cluster operations.** The available configuration items are included for advanced users who need to tune the default configuration for specific workloads.

Example Cassandra configuration:

``` json
{
    "cassandra": {
        "jmx_port": 7199,
        "hinted_handoff_enabled": true,
        "max_hint_window_in_ms": 10800000,
        "hinted_handoff_throttle_in_kb": 1024,
        "max_hints_delivery_threads": 2,
        "batchlog_replay_throttle_in_kb": 1024,
        "key_cache_save_period": 14400,
        "row_cache_size_in_mb": 0,
        "row_cache_save_period": 0,
        "commitlog_sync_period_in_ms": 10000,
        "commitlog_segment_size_in_mb": 32,
        "concurrent_reads": 16,
        "concurrent_writes": 32,
        "concurrent_counter_writes": 16,
        "memtable_allocation_type": "heap_buffers",
        "index_summary_resize_interval_in_minutes": 60,
        "storage_port": 7000,
        "start_native_transport": true,
        "native_transport_port": 9042,
        "tombstone_warn_threshold": 1000,
        "tombstone_failure_threshold": 100000,
        "column_index_size_in_kb": 64,
        "batch_size_warn_threshold_in_kb": 5,
        "batch_size_fail_threshold_in_kb": 50,
        "compaction_throughput_mb_per_sec": 16,
        "sstable_preemptive_open_interval_in_mb": 50,
        "read_request_timeout_in_ms": 5000,
        "range_request_timeout_in_ms": 10000,
        "write_request_timeout_in_ms": 2000,
        "counter_write_request_timeout_in_ms": 5000,
        "cas_contention_timeout_in_ms": 1000,
        "truncate_request_timeout_in_ms": 60000,
        "request_timeout_in_ms": 1000,
        "dynamic_snitch_update_interval_in_ms": 100,
        "dynamic_snitch_reset_interval_in_ms": 600000,
        "dynamic_snitch_badness_threshold": 0.1,
        "internode_compression": "all"
    }
}
```

#### Communication Configuration

The IP address of the Cassandra node is determined automatically by the service when the application is deployed. The listen addresses are appropriately bound to the addresses provided to the container where the process runs. The following configuration items allow users to specify the ports on which the service operates.

| Property             | Type    | Description |
| -------------------- | ------- | --------------- |
| jmx_port              | integer | The port on which the application will listen for JMX connections. Remote JMX connections are disabled due to security considerations. |
| storage_port          | integer | The port the application uses for inter-node communication. |
| internode_compression | all,none,dc | If set to all, traffic between all nodes is compressed. If set to dc, traffic between datacenters is compressed. If set to none, no compression is used for internode communication. |
| native_transport_port  | integer | The port the application uses for inter-node communication. |

#### Commit Log Configuration

The DCOS Cassandra service only supports the commitlog_sync model for configuring the Cassandra commit log. In this model a node responds to write requests after writing the request to file system and replicating to the configured number of nodes, but prior to synchronizing the commit log file to storage media. Cassandra will synchronize the data to storage media after a configurable time period. If all nodes in the cluster should fail, at the Operating System level or below, during this window the acknowledged writes will be lost. Note that, even if the JVM crashes, the data will still be available on the nodes persistent volume when the service recovers the node.The configuration parameters below control the window in which data remains acknowledged but has not been written to storage media.

| Property                 | Type    | Description     |
| ------------------------ | ------- | --------------- |
| commitlog_sync_periodInMs  | integer | The time, in ms, between successive calls to the fsync system call. This defines the maximum window between write acknowledgement and a potential data loss. |
| commitlog_segment_size_in_mb | integer | The size of the commit log in MB. This property determines the maximum mutation size, defined as half the segment size. If a mutation's size exceeds the maximum mutation size, the mutation is rejected. Before increasing the commitlog segment size of the commitlog segments, investigate why the mutations are larger than expected. |

#### Column Index Configuration

| Property                 | Type    | Description     |
| ------------------------ | ------- | --------------- |
| column_index_size_in_kb | integer | Index size  of rows within a partition.For very large rows this value can be decreased to increase seek time. If key caching is enabled be careful when increasing this value, as the key cache may become overwhelmed. |
|index_summary_resize_interval_in_minutes| integer |  How frequently index summaries should be re-sampled in minutes. This is done periodically to redistribute memory from the fixed-size pool to SSTables proportional their recent read rates.|


#### Hinted Handoff Configuration

Hinted handoff is the process by which Cassandra recovers consistency when a write occurs and a node that should hold a replica of the data is not available. If hinted handoff is enabled, Cassandra will record the fact that the value needs to be replicated and replay the write when the node becomes available. Hinted handoff is enabled by default. The following table describes how hinted handoff can be configured.

| Property                | Type  | Description     |
| ------------------------ | ------- | ------------------- |
| hinted_handoff_enabled     | boolean | If true, hinted handoff will be used to maintain consistency during node failure. |
| max_hint_window_in_ms        | integer | The maximum amount of time, in ms, that Cassandra will record hints for an unavailable node. |
| max_hint_delivery_threads   | integer | The number of threads that deliver hints. The default value of 2 should be sufficient most use cases. |

#### Dynamic Snitch Configuration

The endpoint snitch for the service is always the `GossipPropertyFileSnitch`, but, in addition to this, Cassandra uses a dynamic snitch to determine when requests should be routed away from poorly performing nodes. The configuration parameters below control the behavior of the snitch.

| Property                 | Type    | Description     |
| ------------------------ | ------- | --------------- |
| dynamic_snitch_badness_threshold | number |  Controls how much worse a poorly performing node has to be before the dynamic snitch prefers other replicas over it. A value of 0.2 means Cassandra continues to prefer the static snitch values until the node response time is 20% worse than the best performing node. Until the threshold is reached, incoming requests are statically routed to the closest replica. | 
| dynamic_snitch_reset_interval_in_ms | integer | Time interval, in ms, to reset all node scores, allowing a bad nodes to recover. |
| dynamic_snitch_update_interval_in_ms | integer | The time interval, in ms, for node score calculation. This is a CPU intensive operation. Reducing this interval should be performed with extreme caution. |

#### Global Key Cache Configuration

The partition key cache is a cache of the partition index for a Cassandra table. It is enabled by setting the parameter when creating the table. Using the key cache instead of relying on the OS page cache can decrease CPU and memory utilization. However, as the value associated with keys in the partition index is not cached along with key, reads that utilize the key cache will still require that row values be read from storage media, through the OS page cache. The following configuraiton items control the system global configuration for key caches.

| Property                 | Type    | Description     |
| ------------------------ | ------- | --------------- |
| key_cache_save_period | integer | The duration in seconds that keys are saved in cache. Saved caches greatly improve cold-start speeds and has relatively little effect on I/O. |
| key_cache_size_in_mb | integer | The maximum size of the key cache in Mb. When no value is set, the cache is set to the smaller of 5% of the available heap, or 100MB. To disable set to 0. |

#### Global Row Cache Configuration

Row caching caches both the key and the associated row in memory. During the read path, when rows are reconstructed from the `MemTable` and `SSTables`, the reconstructed row is cached in memory preventing further reads from storage media until the row is ejected or dirtied. 
Like key caching, row caching is configurable on a per table basis, but it should be used with extreme caution. Misusing row caching can overwhelm the JVM and cause Cassandra to crash. Use row caching under the following conditions: only if you are absolutely certain that doing so will not overwhelm the JVM, the partition you will cache is small, and client applications will read most of the partition all at once.

The following configuration properties are global for all row caches.

| Property                 | Type    | Description     |
| ------------------------ | ------- | --------------- |
| row_cache_save_period | integer | The duration in seconds that rows are saved in cache. Saved caches greatly improve cold-start speeds and has relatively little effect on I/O. |
| row_cache_size_in_mb | integer | The maximum size of the key cache in Mb. Make sure to provide enough space to contain all the rows for tables that will have row caching enabled. |

### Operating System Configuration
In order for Cassandra to function correctly there are several important configuration modifications that need to be performed to the OS hosting the deployment.
#### Time Synchronization
While writes in Cassandra are atomic at the column level, Cassandra only implements last write wins ordering to resolve consistency during concurrent writes to the same column. If system time is not synchronized across DOCS agent nodes, this will result in inconsistencies in the value stored with respect to the ordering of mutations as observed by the client. It is imperative that a mechanism, such as NTP, is used for time synchronization.
#### Configuration Settings
In addition to time synchronization, Cassandra requires OS level configuration settings typical of a production data base server.

|File                                   | Setting                           | Value                 | Reason  |
| ------------------------------------- | --------------------------------- | --------------------- |---------|
| /etc/sysctl.conf | vm.max_map_count | 131702 | Aside from calls to the system malloc implementation, Cassandra uses mmap directly to memory map files. Exceeding the number of allowed memory mappings will cause a Cassandra node to fail. |
| /etc/sysctl.conf | vm.swappiness    | 0 | If the OS swaps out the Cassandra process it can fail to respond to requests resulting in the node being marked down by the cluster. |
| /etc/security/limits.conf | memlock | unlimited | A Cassandra node can fail to load and map SSTables due to insufficient memory mappings into process address space. This will cause the node to terminate.|
| /etc/security/limits.conf | nofile | unlimited | If this value is too low a Cassandra node will terminate due to insufficient file handles. |
| /etc/security/limits.conf, /etc/security/limits.d/90-nproc.conf | nproc | 32768 | A Cassandra node spawns many threads, which go towards kernel nproc count. If nproc is not set appropriately the node will be killed.|

## Connecting Clients

The only supported client for the DSOC Cassandra Service is the Datastax Java CQL Driver. Note that this means that Thrift RPC-based clients are not supported for use with this service and any legacy applications that use this communication mechanism are run at the user's risk.

### Connection Info Using the CLI

The following command can be executed from the cli to retrieve a set of nodes to connect to.

``` bash
dcos cassandra --framework-name=<framework-name> connection
```

### Connection Info Response

The response is as below.

``` json
{
    "address": [
        "10.0.0.47:9042",
        "10.0.0.50:9042",
        "10.0.0.49:9042"
    ],
    "dns": [
         "node-0.cassandra.mesos:9042",
         "node-1.cassandra.mesos:9042",
         "node-2.cassandra.mesos:9042"
    ]
    
}
```

This address JSON array contains a list of valid nodes addresses for nodes in the cluster. 
The dns JSON array contains valid MesosDNS names for the same nodes. For availability 
reasons, it is best to specify multiple nodes in the configuration of the CQL Driver used 
by the application. 

If IP addresses are used, and a Cassandra node is moved to a different IP
address, the address in the list passed to the Cluster configuration of the application 
should be changed. Note that, once the application is connected to the Cluster, moving a 
node to a new IP address will not result in a loss of connectivity. The CQL Driver is 
capable of dealing with topology changes. However, the application's 
configuration should be pointed to the new address the next time the application is 
restarted.

If DNS ames are used, the DNS name will always resolve to correct IP address of the node. 
This is true, even if the node is moved to a new IP address. However, it is important to 
understand the DNS caching behavior of your application. For a Java application using  
the CQL driver, if a SecurityManager is installed the default behavior is to cache a 
successful DNS lookup forever. Therefore, if a node moves, your application will always 
maintain the original address. If no security manager is installed, the default cache 
behavior falls back to an implementation defined timeout. If a node moves in this case, 
the behavior is generally undefined. If you choose to use DNS to resolve entry points to 
the cluster, the safest method is to set networkaddress.cache.ttl to a reasonable value.
As with the IP address method, the CQL driver still detect topology changes and reamin 
connected even if a node moves.

### Configuring the CQL Driver
#### Adding the Driver to Your Application

``` xml
<dependency>
  <groupId>com.datastax.cassandra</groupId>
  <artifactId>cassandra-driver-core</artifactId>
  <version>3.0.0</version>
</dependency>
```

The snippet above is the correct dependency for CQL driver to use with the DCOS Cassandra service. After adding this dependency to your project, you should have access to the correct binary dependencies to interface with the Cassandra cluster.

#### Connecting the CQL Driver.
The code snippet below demonstrates how to connect the CQL driver to the cluster and perform a simple query.

```
Cluster cluster = null;
try {

   List<InetSocketAddress> addresses = Arrays.asList(
       new InetSocketAddress("10.0.0.47", 9042),
       new InetSocketAddress("10.0.0.48", 9042),
       new InetSocketAddress("10.0.0.45", 9042));

    cluster = Cluster.builder()                                                    
            .addContactPointsWithPorts(addresses)
            .build();
    Session session = cluster.connect();                                           

    ResultSet rs = session.execute("select release_version from system.local");   
    Row row = rs.one();
    System.out.println(row.getString("release_version"));                          
} finally {
    if (cluster != null) cluster.close();                                          
}
```

## Managing

### Add a Node
Increase the `NODES` value via Marathon as described in the [Configuration Update](#configuration-update) section. This creates an update plan as described in that section. An additional node will be added as the last block of that plan. After a node has been added, you should run cleanup, as described in the [Cleanup](#cleanup) section. It is safe to delay running cleanup until off-peak hours.

### Node Status

It is sometimes useful to retrieve information about a Cassandra node for the purpose of troubleshooting or to examine the node's properties. You can request that a node report its status using the following command from the CLI.

```bash
dcos cassandra --framework-name=<framewor-name> node status <nodeid>
```
This command queries the node status directly from the node. If the command fails to return, it may indicate that the node is troubled. Here nodeid is the the sequential integer identifier of the node (e.g. 0, 1, 2 , ..., n). The result that will be returned is as below.

```json
{
    "data_center": "dc1",
    "endpoint": "10.0.3.64",
    "gossip_initialized": true,
    "gossip_running": true,
    "host_id": "32bed59f-3100-40fc-8512-a82aef65abb3",
    "joined": true,
    "mode": "NORMAL",
    "native_transport_running": true,
    "rack": "rac1",
    "token_count": 256,
    "version": "2.2.5"
}
```
| Property                 | Type    | Description     |
| ------------------------ | ------- | --------------- |
| data_center | string | The datacenter that the Cassandra node is configured to run in. This has implications for tables created with topology aware replication strategies. The multidatacenter topologies are not currently supported, they will be in future releases.| 
| endpoint | string | The address of the storage service in the Cluster. |
| gossip_initialized | boolean | If true, the node has initialized the internal gossip protocol. This value should be true if the node is healthy|
| gossip_running | boolean | If true, node's gossip protocol is running. This value should be true if the node is healthy|
| host_id | string| The host id that is exposed to as part of Cassandra's internal protocols. This value may be useful when examining the Cassandra process logs in the cluster. Host's are sometimes referenced by id.|
| joined | true | If true, the node has successfully joined the cluster. This value should always be true if the node is healthy.|
| mode | string | The operating mode of the Cassandra node. If the mode is STARTING, JOINING, or JOINED, the node is initializing. If the mode is NORMAL, the node is healthy and ready to receive client requests.|
| native_transport_running | boolean | If true, the node can service requests over the native transport port using the CQL protocol. If this value is not true, then the node is not capable of serving requests to clients.|
|rack| string | The rack assigned to the Cassandra node. This property is important for topology aware replication strategies. For the DCOS Cassandra service all nodes in the cluster should report the same value.|
|token_count | integer | The number of tokens assigned to the node. The Cassandra DCOS service only supports virtual node based deployments. As the resources allocated to each instance are homogenous, the number of tokens assigned to each node is identical and should always be 256.|
|version| string | The version of Cassandra that is running on the node.|

### Node Info

To view general information about a node, the following command my be run from the CLI.
```bash
dcos cassandra --framework-name=<framewor-name> node describe <nodeid>
```
In contrast to the status command, this command requests information from the DCOS Cassandra Service and not the Cassandra node. 

```json
{
    "hostname": "10.0.3.64",
    "id": "node-0_2db151cb-e837-4fef-b17b-fbdcd25fadcc",
    "name": "node-0",
    "mode": "NORMAL",
    "slave_id": "8a22d1e9-bfc6-4969-a6bf-6d54c9d41ee3-S4",
    "state": "TASK_RUNNING"
}
```

| Property                 | Type    | Description     |
| ------------------------ | ------- | --------------- |
| hostname | string | The hostname or ip address of the DCOS agent on which the node is running.|
| id | string |  The DCOS identifier of the task for the Cassandra node.|
| name | string | The name of the Cassandra node. |
| mode | string | The operating mode of the Cassandra node as recorded by the DCOS Cassandra service. This value should be eventually consistent with the mode returned by the status command.|
| slave_id | string | The identifier of the DCOS slave agent where the node is running.|
| state | string | The state of the task for the Cassandra node. If the node is being installed this value may be TASK_STATING or TASK_STARTING. Under normal operating conditions the state should be TASK_RUNNING. The state may be temporarily displayed as TASK_FINISHED during configuration updates or upgrades.|

### Cleanup

Cassandra does not automatically remove data when a node loses part of its partition range. This can occur when nodes are added or removed from the ring. Tun cleanup to remove the unnecessary data. Cleanup can be a CPU- and disk-intensive operation. As such, it is recommended to delay running cleanup until off-peak hours. The DCOS Cassandra service will minimize the aggregate CPU and disk utilization for the cluster by performing cleanup for each selected node sequentially.

To perform a cleanup from the CLI, enter the following command:

``` bash
dcos cassandra --framework-name=<framewor-name> cleanup --nodes=<nodes> --key_spaces=<key_spaces> --column_families=<column_families>
```

Here, `<nodes>` is an optional comma-separated list indicating the nodes to cleanup, `<key_spaces>` is an optional comma-separated list of the key spaces to cleanup, and `<column-families>` is an optional comma-separated list of the column-families to cleanup.
If no arguments are specified a cleanup will be performed for all nodes, key spaces, and column families.

### Repair
Over time the replicas stored in a Cassandra cluster may become out of sync. In Cassandra, hinted handoff and read repair maintain the consistency of replicas when a node is temporarily down and during the data read path. However, as part of regular cluster maintenance, or when a node is replaced, removed, or added, manual anti-entropy repair should be performed. 
Like cleanup, repair can be a CPU and disk intensive operation. When possible, it should be run during off peak hours. To minimize the impact on the cluster, the DCOS Cassandra framework will run a sequential, primary range, repair on each node of the cluster for the selected nodes, key spaces, and column families.

To perform a repair from the CLI, enter the following command:

``` bash
dcos cassandra --framework-name=<framewor-name> repair --nodes=<nodes> --key_spaces=<key_spaces> --column_families=<column_families>
```

Here, `<nodes>` is an optional comma-separated list indicating the nodes to repair, `<key_spaces>` is an optional comma-separated list of the key spaces to repair, and `<column-families>` is an optional comma-separated list of the column-families to repair.
If no arguments are specified a repair will be performed for all nodes, key spaces, and column families.
 
### Backup and Restore

DCOS Cassandra supports backup and restore from S3 storage for disaster recovery purposes.

Cassandra takes a snapshot your tables and ships them to a remote location. Once the snapshots have been uploaded to a remote location, you can restore the data to a new cluster, in the event of a disaster, or restore them to an existing cluster, in the event that a user error has caused a data loss.

#### Backup

You can take a complete snapshot of your DCOS Cassandra ring and upload the artifacts to S3.

To perform a backup, enter the following command on the DCOS CLI:

``` bash
dcos cassandra --framework-name=<framework-name> backup start \
    --name=<backup-name> \
    --external_location=s3://<bucket-name> \
    --s3_access_key=<s3-access-key> \
    --s3_secret_key=<s3-secret-key>
```

Check status of the backup:

``` bash
dcos cassandra --framework-name=<framewor-name> backup status
```

#### Restore

You can restore your DCOS Cassandra snapshots on a new Cassandra ring.

To restore, enter the following command on the DCOS CLI:

``` bash
dcos cassandra --framework-name=<framework-name> restore start \
    --name=<backup-name> \
    --external_location=s3://<bucket-name> \
    --s3_access_key=<s3-access-key> \
    --s3_secret_key=<s3-secret-key>
```

Check the status of the restore:

``` bash
dcos cassandra --framework-name=<framework-name> restore status
```

## Troubleshooting

###Configuration Update Errors
The plan below shows shows contains a configuration error that will not allow the installation or configuration update to progress.

``` json
{
    "errors": ["The number of seeds is greater than the number of nodes."], 
    "phases": [
        {
            "blocks": [
                {
                    "has_decision_point": false, 
                    "id": "738122a7-8b52-4d45-a2b0-41f625f04f87", 
                    "message": "Reconciliation complete", 
                    "name": "Reconciliation", 
                    "status": "Complete"
                }
            ], 
            "id": "0836a986-835a-4811-afea-b6cb9ddcd929", 
            "name": "Reconciliation", 
            "status": "Complete"
        }, 
        {
            "blocks": [
                {
                    "has_decision_point": false, 
                    "id": "440485ec-eba2-48a3-9237-b0989dbe9f68", 
                    "message": "Deploying Cassandra node node-0", 
                    "name": "node-0", 
                    "status": "Pending"
                }, 
                {
                    "has_decision_point": false, 
                    "id": "84251eb9-218c-4700-a03c-50018b90d5a8", 
                    "message": "Deploying Cassandra node node-1", 
                    "name": "node-1", 
                    "status": "Pending"
                }, 
                {
                    "has_decision_point": false, 
                    "id": "aad765fe-5aa5-4d4e-bf66-abbb6a15e125", 
                    "message": "Deploying Cassandra node node-2", 
                    "name": "node-2", 
                    "status": "Pending"
                }
            ], 
            "id": "c4f61c72-038d-431c-af73-6a9787219233", 
            "name": "Deploy", 
            "status": "Pending"
        }
    ], 
    "status": "Error"
}
```
To proceed with the installation or configuration update fix the indicated errors by updating the configuration as detailed in the [Configuration Update](#configuration-update) section.

###Replacing a Permanently Failed Node
The DCOS Cassandra Service is resilient to temporary node failures. However, if a DCOS agent hosting a Cassandra node is permanently lost, manual intervention is required to replace the failed node. The following command should be used to replace the node residing on the failed server.

``` bash
dcos cassandra --framework-name=<framework-name> node replace <node_id>
```

This will replace the node with a new node of the same name running on a different server. The new node will take over the token range owned by its predecessor. After replacing a failed node, you should run [Cleanup]

## API Reference
The DCOS Cassandra Service implements a REST API that may be accessed from outside the cluster. If the DCOS cluster is configured with OAuth enabled, then you must acquire a valid token and include that token in the Authorization header of all requests. The <auth_token> parameter below is used to represent this token. 
The <dcos_url> parameter referenced below indicates the base URL of the DCOS cluster on which the Cassandra Service is deployed. Depending on the transport layer security configuration of your deployment this may be a HTTP or a HTTPS URL.

###Configuration

#### View the Installation Plan

``` bash
curl -H "Authorization:token=<auth_token>" <dcos_url>/service/cassandra/v1/plan
```

#### Retrieve Connection Info

``` bash
curl -H "Authorization:token=<auth_token>" <dcos_url>/cassandra/v1/nodes/connect
```

You will see a response similar to the following:

``` json
{
    "nodes": [
        "10.0.0.47:9042",
        "10.0.0.50:9042",
        "10.0.0.49:9042"
    ]
}
```

This JSON array contains a list of valid nodes that the client can use to connect to the Cassandra cluster. For availability reasons, it is best to specify multiple nodes in the CQL Driver configuration used by the application.

#### Pause Installation

The installation will pause after completing installation of the current node and wait for user input.

``` bash
curl -X PUT -H "Authorization:token=<auth_token>" <dcos_url>/service/cassandra/v1/plan?cmd=interrupt
```

#### Resume Installation

The REST API request below will resume installation at the next pending node.

``` bash
curl -X PUT <dcos_surl>/service/cassandra/v1/plan?cmd=proceed
```

### Managing

#### Node Status
The status of a node can be retrieved by sending a GET request to `/v1/nodes/status`.

``` bash
curl  -H "Authorization:token=<auth_token>" <dcos_url>/service/cassandra/v1/status
```

#### Node Info
Node information can be retrieved by sending a GET request to `/v1/nodes/info`.

``` bash
curl  -H "Authorization:token=<auth_token>" <dcos_url>/service/cassandra/v1/status
```

#### Cleanup

First, create the request payload, for example, in a file `cleanup.json`:

``` json
{
    "nodes":["*"],
    "key_spaces":["my_keyspace"],
    "column_families":["my_cf_1", "my_cf_w"]
}
```
In the above, the nodes list indicates the nodes on which cleanup will be performed. The value [*], indicates to perform the cleanup cluster wide. key_spaces and column_families indicate the key spaces and column families on which cleanup will be performed. These may be ommitted if all key spaces and/or all column families should be targeted. The json below shows the request payload for a cluster wide cleanup operation of all key spaces and column families.

``` json
{
    "nodes":["*"]
}
```

``` bash
curl -X PUT -H "Content-Type:application/json" -H "Authorization:token=<auth_token>" <dcos_url>/service/cassandra/v1/cleanup/start --data @cleanup.json
```

#### Repair

First, create the request payload, for example, in a file `repair.json`:

``` json
{
    "nodes":["*"],
    "key_spaces":["my_keyspace"],
    "column_families":["my_cf_1", "my_cf_w"]
}
```
In the above, the nodes list indicates the nodes on which the repair will be performed. The value [*], indicates to perform the repair cluster wide. key_spaces and column_families indicate the key spaces and column families on which repair will be performed. These may be ommitted if all key spaces and/or all column families should be targeted. The json below shows the request payload for a cluster wide repair operation of all key spaces and column families.

``` json
{
    "nodes":["*"]
}
```

``` bash
curl -X PUT -H "Content-Type:application/json" -H "Authorization:token=<auth_token>" <dcos_url>/service/cassandra/v1/repair/start --data @repair.json
```

#### Backup

First, create the request payload, for example, in a file `backup.json`:

``` json
{
    "name":"<backup-name>",
    "external_location":"s3://<bucket-name>",
    "s3_access_key":"<s3-access-key>",
    "s3_secret_key":"<s3-secret-key>"
}
```

Then, submit the request payload via `PUT` request to `/v1/backup/start`

``` bash
curl -X PUT -H "Content-Type: application/json" -H "Authorization:token=<auth_token>" -d @backup.json <dcos_url>/service/cassandra/v1/backup/start
{"status":"started", message:""}
```

Check status of the backup:

``` bash
curl -X GET http://cassandra.marathon.mesos:9000/v1/backup/status
```

#### Restore

First, bring up a new instance of your Cassandra cluster with the same number of nodes as the cluster whose snapshot backup you want to restore.

Next, create the request payload, for example, in a file `restore.json`:

``` json
{
    "name":"<backup-name-to-restore>",
    "external_location":"s3://<bucket-name-where-backups-are-stored>",
    "s3_access_key":"<s3-access-key>",
    "s3_secret_key":"<s3-secret-key>"
}
```

Next, submit the request payload via `PUT` request to `/v1/restore/start`

``` bash
curl -X PUT -H "Content-Type: application/json" -H "Authorization:token=<auth_token>" -d @restore.json <dcos_url>/service/cassandra/v1/restore/start
{"status":"started", message:""}
```

Check status of the restore:

``` bash
curl -X -H "Authorization:token=<auth_token>" <dcos_url>/service/cassandra/v1/restore/status
```

## Limitations

- Cluster backup and restore can only be performed sequentially across the entire cluster. While this makes cluster backup and restore time consuming, it also ensures that taking backups and restoring them will not overwhelm the cluster or the network. In the future, DCOS Cassandra could allow for a user-specified degree of parallelism when taking backups.
- Cluster restore can only restore a cluster of the same size as, or larger than, the cluster from which the backup was taken.
- While nodes can be replaced, there is currently no way to shrink the size of the cluster. Future releases will contain decommissions and remove operations.
- Anti-entropy repair can only be performed sequentially, for the primary range of each node, across the entire cluster. There are use cases where one might wish to repair an individual node, but running the repair procedure as implemented is always sufficient to repair the cluster.

## Development

View the [development guide](https://github.com/mesosphere/dcos-cassandra-service/blob/master/dev-guide.md).
