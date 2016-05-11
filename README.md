# Overview

[![Build Status](http://velocity.mesosphere.com/service/velocity/buildStatus/icon?job=infinity-cassandra)](http://velocity.mesosphere.com/service/velocity/job/infinity-cassandra/)

DCOS Cassandra is an automated service that makes it easy to deploy and manage on Mesosphere DCOS. DCOS Cassandra eliminates nearly all of the complexity traditional associated with managing a Cassandra cluster. Apache Cassandra is distributed database management system designed to handle large amounts of data across many nodes, providing horizonal scalablity and high availability with no single point of failure, with a simple query language (CQL). For more information on Apache Cassandra, see the Apache Cassandra [documentation] (http://docs.datastax.com/en/cassandra/2.2/pdf/cassandra22.pdf). DCOS Cassandra gives you direct access to the Cassandra API so that existing applications can interoperate. You can configure and install DCOS Cassandra in moments. Multiple Cassandra clusters can be installed on DCOS and managed independently, so you can offer Cassandra as a managed service to your organization.

## Benefits

DCOS Cassandra offers the following benefits:

- Easy installation
- Multiple Cassandra clusters
- Elastic cluster scaling
- Replication for high availability
- Integrated monitoring


## Features

DCOS Cassandra provides the following features:

- Single command installation for rapid provisioning
- Persistent storage volumes for enhanced data durability
- Runtime configuration and software updates for high availability
- Health checks and metrics for monitoring
- Backup and restore for disaster recovery
- Cluster wide automation Cleanup and Repair

## Related Services

- [DCOS Spark](https://docs.mesosphere.com/manage-service/spark)

# Getting Started

## Quick Start

* Step 1. Install a Cassandra cluster using DCOS CLI:

**Note:** Your cluster must have at least 3 private nodes.

```
$ dcos package install cassandra
```

* Step 2. Once the cluster is installed, retrieve connection information by running the `connection` command:

```
$ dcos cassandra connection
{
    "address": [
        "10.0.2.136:9042",
        "10.0.2.138:9042",
        "10.0.2.137:9042"
    ],
    "dns": [
         "node-0.cassandra.mesos:9042",
         "node-1.cassandra.mesos:9042",
         "node-2.cassandra.mesos:9042"
    ]
   
}
```

* Step 3. [SSH into a DC/OS node](https://docs.mesosphere.com/administration/sshcluster/):

```
$ dcos node ssh --master-proxy --leader
core@ip-10-0-6-153 ~ $
```

Now that you are inside your DC/OS cluster, you can connect to your Cassandra cluster directly.

* Step 4. Launch a docker container containing `cqlsh` to connect to your cassandra cluster. Use one of the nodes you retrieved from the `connection` command:

```
core@ip-10-0-6-153 ~ $ docker run -ti cassandra:2.2.5 cqlsh 10.0.2.136
cqlsh>
```

* Step 5. You are now connected to your Cassandra cluster. Create a sample keyspace called `demo`:

```
cqlsh> CREATE KEYSPACE demo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
```

* Step 6. Create a sample table called `map` in our `demo` keyspace:

```
cqlsh> USE demo;CREATE TABLE map (key varchar, value varchar, PRIMARY KEY(key));
```

* Step 7. Insert some data in the table:

```
cqlsh> INSERT INTO demo.map(key, value) VALUES('Cassandra', 'Rocks!');
cqlsh> INSERT INTO demo.map(key, value) VALUES('StaticInfrastructure', 'BeGone!');
cqlsh> INSERT INTO demo.map(key, value) VALUES('Buzz', 'DC/OS is the new black!');
```    

* Step 8. Query the data back to make sure it persisted correctly:

```
cqlsh> SELECT * FROM demo.map;
```    

## Install and Customize

### Default Installation
Prior to installing a default cluster, ensure that your DCOS cluster has at least 3 DCOS slaves with 8 Gb of memory, 10 Gb of disk available on each agent. Also, ensure that ports 7000, 7001, 7199, 9042, and 9160 are available.

To start a the default cluster, run the following command on the DCOS CLI. The default installation may not be sufficient for a production deployment, but all cluster operations will work. If you are planning a production deployment with 3 replicas of each value and with local quorum consistency for read and write operations (a very common use case), this configuration is sufficient for development and testing purposes and it may be scaled to a production deployment.

```
$ dcos package install cassandra
```

This command creates a new Cassandra cluster with 3 nodes. Two clusters cannot share the same name, so installing additional clusters beyond the default cluster requires [customizing the `name` at install time](#custom-installation) for each additional instance.

If you have more than one Cassandra cluster, use the `--name` argument after install time to specify which Cassandra instance to query. All `dcos cassandra` CLI commands accept the `--name` argument. If you do not specify a service name, the CLI assumes the default value, `cassandra`.

### Custom Installation

If you are ready to ship into production, you will likely need to customize the deployment to suite the workload requirements of your application(s). Customize the default deployment by creating a JSON file, then pass it to `dcos package install` using the `--options` parameter.

Sample JSON options file named `sample-cassandra.json`:

```
{
    "nodes": {
        "count": 10,
        "seeds": 3
    }
}
```

The command below creates a cluster using `sample-cassandra.json`:

```
$ dcos package install --options=sample-cassandra.json cassandra
```

This cluster will have 10 nodes and 3 seeds instead of the default values of 3 nodes and 2 seeds.
See [Configuration Options](#configuration-options) for a list of fields that can be customized via an options JSON file when the Cassandra cluster is created.

### Minimal Installation
You may wish to install Cassandra on a local DCOS cluster for development or testing purposes. For this, you can use [dcos-vagrant](https://github.com/mesosphere/dcos-vagrant).
As with the default installation, you must ensure that ports 7000, 7001,7 199, 9042, and 9160 are available.

**Note:** This configuration will not support replication of any kind, but it may be sufficient for early stage evaluation and development.

To start a minimal cluster with a single node, create a JSON options file that contains the following:

```
{
    "service" : {
       "cpus": 0.1,
       "mem": 512,
       "heap": 256
    },
    "nodes": {
        "cpus": 0.5,
        "mem": 2048,
        "disk": 4096,
        "heap": {
            "size": 1024,
            "new": 100
        },
        "count": 1,
        "seeds": 1
    },
    "executor" : {
       "cpus": 0.1,
       "mem": 512,
       "heap": 256
    },
    "task" : {
       "cpus": 0.1,
       "mem": 128,
    }
}
```
This will create a single node cluster with 2 Gb of memory and 4Gb of disk. Note that you will need an additional 512 Mb for the DCOS Cassandra Service executor and 128 Mb for clusters tasks. The DCOS Cassandra Service scheduler needs 512 MB to run, but it does not need to be deployed on the same host as the node.

## Multiple Cassandra Cluster Installation

Installing multiple Cassandra clusters is identical to installing a Cassandra cluster with a custom configuration as described above. Use a JSON options file to specify a unique `name` for each installation:

```
$ cat cassandra1.json
{
   "service": {
       "name": "cassandra1"
   }
}

$ dcos package install cassandra --options=cassandra1.json
```

In order to avoid port conflicts, by default you cannot collocate more than one Cassandra instance on the same node.

### Installation Plan

When the DCOS Cassandra service is initially installed it will generate an installation plan as shown below. 

```
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
	        "id": "e90ad90b-fd71-4a1d-a63b-599003ea46f5",
	         "name": "Sync Data Center",
	         "blocks": [],
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

#### Viewing the Installation Plan
The plan can be viewed from the API via the REST endpoint. A curl example is provided below.

```
$ curl http://<dcos_url>/service/cassandra/v1/plan
```

If you are using Enterprise DC/OS, use the following command to view the installation plan:

```
curl -v -H "Authorization: token=$(dcos config show core.dcos_acs_token)" http://<dcos_url>/service/cassandra/v1/plan/
```

If you are using the Enterprise Edition of DCOS with Authentication enabled you will need to include the token in the POST command.

```
curl -v -H "Authorization: token=$(dcos config show core.dcos_acs_token)" http://<dcos_url>/service/cassandra/v1/plan
```

#### Plan Errors
If there are any errors that prevent installation, these errors are dispayed in the errors list. The presence of errors indicates that the installation cannot progress. See the [Troubleshooting](#troubleshooting) section for information on resolving errors.

#### Reconciliation Phase
The first phase of the installation plan is the reconciliation phase. This phase ensures that the DCOS Cassandra service maintains the correct status for the Cassandra nodes that it has deployed. Reconciliation is a normal operation of the DCOS Cassandra Service and occurs each time the service starts. See [the Mesos documentation](http://mesos.apache.org/documentation/latest/reconciliation) for more information.

#### Deploy Phase
The second phase of the installation is the deploy phase. This phase will deploy the requested number of Cassandra nodes. Each block in the phase represents an individual Cassandra node. In the plan shown above the first node, node-0, has been deployed, the second node, node-1, is in the process of being deployed, and the third node, node-2, is pending deployment based on the completion of node-1.

#### Pausing Installation
In order to pause installation, issue a REST API request as shown below. The installation will pause after completing installation of the current node and wait for user input.

```
$ curl -X POST http:/<dcos_url>/service/cassandra/v1/plan/interrupt
```

If you are using the Enterprise Edition of DCOS with Authentication enabled you will need to include the token in the POST command.

```
curl -v -H "Authorization: token=$(dcos config show core.dcos_acs_token)" -X POST http://<dcos_url>/service/cassandra/v1/plan/interrupt
```

#### Resuming Installation
If the installation has been paused, the REST API request below will resume installation at the next pending node.

```
$ curl -X POST http://<dcos_url>/service/cassandra/v1/plan/continue
```

If you are using the Enterprise Edition of DCOS with Authentication enabled you will need to include the token in the POST command.

```
curl -v -H "Authorization: token=$(dcos config show core.dcos_acs_token)" -X POST http://<dcos_url>/service/cassandra/v1/plan/continue
```

## Uninstall

Uninstalling a cluster is straightforward. Replace `cassandra` with the name of the Cassandra instance to be uninstalled.

```
$ dcos package uninstall --app-id=cassandra
```

Then, use the [framework cleaner script](https://docs.mesosphere.com/framework_cleaner/) to remove your Cassandra instance from Zookeeper and destroy all data associated with it. The arguments the script requires are derived from your service name:

- `framework_role` is `<service-name>_role`.
- `framework_principal` is `<service-name>_principal`.
- `zk_path` is `<service-name>`.

# Configuring

## Changing Configuration at Runtime

You can customize your cluster in-place when it is up and running.

The Cassandra scheduler runs as a Marathon process and can be reconfigured by changing values within Marathon. These are the general steps to follow:

1. View your Marathon dashboard at `http://<dcos_url>/marathon`
2. In the list of `Applications`, click the name of the Cassandra service to be updated.
3. Within the Cassandra instance details view, click the `Configuration` tab, then click the `Edit` button.
4. In the dialog that appears, expand the `Environment Variables` section and update any field(s) to their desired value(s). For example, to increase the number of nodes, edit the value for `NODES`. Click `Change and deploy configuration` to apply any changes and cleanly reload the Cassandra scheduler. The Cassandra cluster itself will persist across the change.

### Configuration Deployment Strategy

Configuration updates are rolled out through execution of Update Plans. You can configure the way these plans are executed.

#### Configuration Update Plans 
This configuration update strategy is analogous to the installation procedure above. If the configuration update is accepted, there will be no errors in the generated plan, and a rolling restart will be performed on all nodes to apply the updated configuration. However, the default strategy can be overridden by a strategy the user provides.

## Configuration Update

Make the REST request below to view the current plan:

```
$ curl -v http://<dcos_url>/service/cassandra/v1/plan
```

The response will look similar to this:

```
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
	        "id": "e90ad90b-fd71-4a1d-a63b-599003ea46f5",
	         "name": "Sync Data Center",
	         "blocks": [],
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

```
$ curl -X POST http:/<dcos_url>/service/cassandra/v1/plan/interrupt
```


If you query the plan again, the response will look like this (notice `status: "Waiting"`):

```
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
	        "id": "e90ad90b-fd71-4a1d-a63b-599003ea46f5",
	         "name": "Sync Data Center",
	         "blocks": [],
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

```
$ curl -X POST http://<dcos_url>/service/cassandra/v1/plan/continue
```

After you execute the continue operation, the plan will look like this:

```
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
	        "id": "e90ad90b-fd71-4a1d-a63b-599003ea46f5",
	         "name": "Sync Data Center",
	         "blocks": [],
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

## Configuration Options

The following describes the most commonly used features of DCOS Cassandra and how to configure them via the DCOS CLI and in Marathon. There are two methods of configuring a Cassandra cluster. The configuration may be specified using a JSON file during installation via the DCOS command line (See the [Installation section](#installation)) or via modification to the Service Scheduler’s Marathon environment at runtime (See the [Configuration Update section](#configuration-update)). Note that some configuration options may only be specified at installation time, but these generally relate only to the service’s registration and authentication with the DCOS scheduler.

### Service Configuration

The service configuration object contains properties that MUST be specified during installation and CANNOT be modified after installation is in progress. This configuration object is similar across all DCOS Infinity services. Service configuration example:

```
{
    "service": {
        "name": "cassandra2",
        "cluster" : "dcos_cluster",
        "role": "cassandra_role",
        "data_center" : "dc2",
        "principal": "cassandra_principal",
        "secret" : "/path/to/secret_file",
        "cpus" : 0.5,
        "mem" : 2048,
        "heap" : 1024,
        "api_port" : 9000,
        "data_center_url":"http://cassandra2.marathon.mesos:9000/v1/cassandra/datacenter"
        "external_data_centers":"http://cassandra.marathon.mesos:9000/v1/cassandra/datacenter"
    }
}
```

<table class="table">
  <tr>
    <th>Property</th>
    <th>Type</th>
    <th>Description</th>
  </tr>

  <tr>
    <td>name</td>
    <td>string</td>
    <td>The name of the Cassandra service installation. This must be unique for each DCOS Cassandra service instance deployed on a DCOS cluster.</td>
  </tr>
  
  <tr>
    <td>cluster</td>
    <td>string</td>
    <td>The cluster that the Cassandra service installation belongs to. Multiple DCOS Cassandra service instances may belong to the same cluster.</td>
  </tr>
  
  <tr>
    <td>data_center</td>
    <td>string</td>
    <td>The identifier of the data center that the DCOS Cassandra service will deploy. This MUST be unique for deployments supporting multiple data centers. This 
    MAY be identical for multiple deployments on the same DCOS cluster that support different clusters.</td>
  </tr>

  <tr>
    <td>user</td>
    <td>string</td>
    <td>The name of the operating system user account Cassandra tasks run as.</td>
  </tr>
  
  <tr>
    <td>principal</td>
    <td>string</td>
    <td>The authentication principal for the Cassandra cluster.</td>
  </tr>
  <tr>
    <td>placement_strategy</td>
    <td>string</td>
    <td>The name of the placement strategy of the Cassandra nodes.</td>
  </tr>
  
  <tr>
    <td>secret</td>
    <td>string</td>
    <td>An optional path to the file containing the secret that the service will use to authenticate with the Mesos Master in the DCOS cluster. This parameter is optional, and should be omitted unless the DCOS deployment is specifically configured for authentication.</td>
  </tr>
  
   <tr>
      <td>cpus</td>
      <td>number</td>
      <td>The number of CPU shares allocated to the DCOS Cassandra Service scheduler. </td>
    </tr>
    
    <tr>
      <td>mem</td>
      <td>integer</td>
      <td>The amount of memory, in MB, allocated for the DCOS Cassandra Service scheduler. This MUST be larger than the allocated heap. 2 Gb is a good choice.</td>
    </tr>
    
    <tr>
      <td>heap</td>
      <td>integer</td>
      <td>The amount of heap, in MB, allocated for the DCOS Cassandra Service scheduler. 1 Gb is a minimum for production installations.</td>
    </tr>
    
    <tr>
      <td>api_port</td>
      <td>integer</td>
      <td>The port that the scheduler will accept API requests on.</td>
    </tr>
    
    <tr>
    <td>data_center_url</td>
    <td>string</td>
    <td>This specifies the URL that the DCOS Cassandra service instance will advertise to other instances in the cluster. 
    If you are not configuring a multi data center deployment this should be omitted. 
    If you are configuring a multiple data center deployment inside the same DCOS cluster, this should be omitted. 
    If you are configuring a multiple data center deployment inside diffrent DCOS clusters, this value MUST be set to a URL that 
    is reachable and resolvable by the DCOS Cassandra instances in the remote data centers. A good choice for this value is the admin 
    router URL (i.e. <dcos_url>/service/cassandra/v1/datacenter).  </td>
    </tr>
    <tr>
    <td>data_center_url</td>
    <td>string</td>
    <td>This specifies the URLs of the external data centers that contain a cluster the DCOS Cassandra service will join as a comma separated list. 
    This value should only be included when your deploying a DCOS Cassandra service instance that will extend an existing cluster. Otherwise, this 
    value should be omitted. If this value is specified, the URLs contained in the comma separated list MUST be resolvable and reachable from the deployed cluster.
    In practice, they should be identical to the values specified in data_center_url configuration parameter for the instance whose cluster will be extended. 
    </td>
  </tr>
  
</table>

- **In the DCOS CLI, options.json**: `name` = string (default: `cassandra`)
- **In Marathon**: The service name cannot be changed after the cluster has started.

### Node Configuration

The node configuration object corresponds to the configuration for Cassandra nodes in the Cassandra cluster. Node configuration MUST be specified during installation and MAY be modified during configuration updates. All of the properties except for `disk` MAY be modified during the configuration update process.

Example node configuration:
```
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

<table class="table">

  <tr>
    <th>Property</th>
    <th>Type</th>
    <th>Description</th>
  </tr>
  
   <tr>
    <td>cpus</td>
    <td>number</td>
    <td>The number of cpu shares allocated to the container where the Cassandra process resides. Currently, due to a bug in Mesos, it is not safe to modify this parameter.</td>
  </tr>
  
  <tr>
    <td>mem</td>
    <td>integer</td>
    <td>The amount of memory, in MB, allocated to the container where the Cassandra process resides. This value MUST be larger than the specified max heap size. Make sure to allocate enough space for additional memory used by the JVM and other overhead.</td>
  </tr>
  
  <tr>
    <td>disk</td>
    <td>integer</td>
    <td>The amount of disk, in MB, allocated to a Cassandra node in the cluster. **Note:** Once this value is configured, it can not be changed.</td>
  </tr>
  
  <tr>
    <td>disk_type</td>
    <td>string</td>
    <td>The type of disk to use for storing Cassandra data. Possible values: <b>ROOT</b> (default) and <b>MOUNT</b>. <b>Note:</b> Once this value is configured, it can not be changed.
    <ul>
    <li><b>ROOT:</b> Cassandra data is stored on the same volume as the agent work directory. And, the Cassandra node tasks will use the configured amount of <i>disk</i> space.</li>
    <li><b>MOUNT:</b> Cassandra data will be stored on a dedicated volume attached to the agent. Dedicated MOUNT volumes have performance advantages and a disk error on these MOUNT volumes will be correctly reported to Cassandra.</li>
    </ul>
    </td>
  </tr>
  
  <tr>
    <td>heap.size</td>
    <td>integer</td>
    <td>The maximum and minimum heap size used by the Cassandra process in MB. This value SHOULD be at least 2 GB, and it SHOULD be no larger than 80% of the allocated memory for the container. Specifying very large heaps, greater than 8 GB, is currently not a supported configuration.</td>
  </tr>
  
  <tr>
    <td>heap.new</td>
    <td>integer</td>
    <td>The young generation heap size in MB. This value should be set at roughly 100MB per allocated CPU core. Increasing the size of this value will generally increase the length of garbage collection pauses. Smaller values will increase the frequency of garbage collection pauses.</td>
  </tr>
  
  <tr>
    <td>count</td>
    <td>integer</td>
    <td>The number of nodes in the Cassandra cluster. This value MUST be between 3 and 100.</td>
  </tr>
  
  <tr>
    <td>seeds</td>
    <td>integer</td>
    <td>The number of seed nodes that the service will use to seed the cluster. The service selects seed nodes dynamically based on the current state of the cluster. 2 - 5 seed nodes is generally sufficient.</td>
  </tr>
  
</table>

### Executor Configuration
The executor configuration object allows you modify the resources associated with the DCOS Cassandra Service's executor. These properties should not be modified unless you are trying to install a small cluster in a resource constrained environment.
Example executor configuration:
```
{
    "executor": {
        "cpus": 0.5,
        "mem": 1024,
        "heap" : 768,
        "disk": 1024,
        "api_port": 9001
    }
}
```

<table class="table">
    <tr>
    <th>Property</th>
    <th>Type</th>
    <th>Description</th>
  </tr>
   <tr>
      <td>cpus</td>
      <td>number</td>
      <td>The number of CPU shares allocated to the DCOS Cassandra Service executor. </td>
    </tr>
    
    <tr>
      <td>mem</td>
      <td>integer</td>
      <td>The amount of memory, in MB, allocated for the DCOS Cassandra Service scheduler. This MUST be larger than the allocated heap.</td>
    </tr>
    
    <tr>
      <td>heap</td>
      <td>integer</td>
      <td>The amount of heap, in MB, allocated for the DCOS Cassandra Service executor.</td>
    </tr>
    
    <tr>
      <td>disk</td>
      <td>integer</td>
      <td>The amount of disk, in MB, allocated for the DCOS Cassandra Service executor.</td>
    </tr>
    
    <tr>
      <td>api_port</td>
      <td>integer</td>
      <td>The port that the executor will accept API requests on.</td>
    </tr>
  
</table>
### Task Configuration
The task configuration object allows you to modify the resources associated with management operations.  Again, These properties should not be modified unless you are trying to install a small cluster in a resource constrained environment.
Example executor configuration:
```
{
    "task": {
        "cpus": 1.0,
        "mem": 256
    }
}
```
<table class="table">
    <tr>
    <th>Property</th>
    <th>Type</th>
    <th>Description</th>
  </tr>
   <tr>
      <td>cpus</td>
      <td>number</td>
      <td>The number of CPU shares allocated to the DCOS Cassandra Service tasks. </td>
    </tr>
    <tr>
      <td>mem</td>
      <td>integer</td>
      <td>The amount of memory, in MB, allocated for the DCOS Cassandra Service tasks.</td>
    </tr>
</table>
### Cassandra Application Configuration

The Cassandra application is configured via the Cassandra JSON object. **You should not modify these settings without strong reason and an advanced knowledge of Cassandra internals and cluster operations.** The available configuration items are included for advanced users who need to tune the default configuration for specific workloads.

Example Cassandra configuration:

```
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

### Communication Configuration

The IP address of the Cassandra node is determined automatically by the service when the application is deployed. The listen addresses are appropriately bound to the addresses provided to the container where the process runs. The following configuration items allow users to specify the ports on which the service operates.

<table class="table">

  <tr>
    <th>Property</th>
    <th>Type</th>
    <th>Description</th>
  </tr>
  
   <tr>
    <td>jmx_port</td>
    <td>integer</td>
    <td>The port on which the application will listen for JMX connections. Remote JMX connections are disabled due to security considerations.</td>
  </tr>
  
   <tr>
    <td>storage_port</td>
    <td>integer</td>
    <td>The port the application uses for inter-node communication.</td>
  </tr>
  
   <tr>
    <td>internode_compression</td>
    <td>all,none,dc</td>
    <td>If set to all, traffic between all nodes is compressed. If set to dc, traffic between datacenters is compressed. If set to none, no compression is used for internode communication.</td>
  </tr>
  
  <tr>
    <td>native_transport_port</td>
    <td>integer</td>
    <td>The port the application uses for inter-node communication.</td>
  </tr>
  
</table>

### Commit Log Configuration

The DCOS Cassandra service only supports the commitlog_sync model for configuring the Cassandra commit log. In this model a node responds to write requests after writing the request to file system and replicating to the configured number of nodes, but prior to synchronizing the commit log file to storage media. Cassandra will synchronize the data to storage media after a configurable time period. If all nodes in the cluster should fail, at the Operating System level or below, during this window the acknowledged writes will be lost. Note that, even if the JVM crashes, the data will still be available on the nodes persistent volume when the service recovers the node.The configuration parameters below control the window in which data remains acknowledged but has not been written to storage media.

<table class="table">

  <tr>
    <th>Property</th>
    <th>Type</th>
    <th>Description</th>
  </tr>
  
   <tr>
    <td>commitlog_sync_period_in_ms</td>
    <td>integer</td>
    <td>The time, in ms, between successive calls to the fsync system call. This defines the maximum window between write acknowledgement and a potential data loss.</td>
  </tr>
  
   <tr>
    <td>commitlog_segment_size_in_mb</td>
    <td>integer</td>
    <td>The size of the commit log in MB. This property determines the maximum mutation size, defined as half the segment size. If a mutation's size exceeds the maximum mutation size, the mutation is rejected. Before increasing the commitlog segment size of the commitlog segments, investigate why the mutations are larger than expected.</td>
  </tr>

</table>

### Column Index Configuration

<table class="table">

  <tr>
    <th>Property</th>
    <th>Type</th>
    <th>Description</th>
  </tr>
  
   <tr>
    <td>column_index_size_in_kb</td>
    <td>integer</td>
    <td>Index size  of rows within a partition. For very large rows, this value can be decreased to increase seek time. If key caching is enabled be careful when increasing this value, as the key cache may become overwhelmed.</td>
  </tr>
  
  <tr>
    <td>index_summary_resize_interval_in_minutes</td>
    <td>integer</td>
    <td>How frequently index summaries should be re-sampled in minutes. This is done periodically to redistribute memory from the fixed-size pool to SSTables proportional their recent read rates.</td>
  </tr>

</table>

### Hinted Handoff Configuration

Hinted handoff is the process by which Cassandra recovers consistency when a write occurs and a node that should hold a replica of the data is not available. If hinted handoff is enabled, Cassandra will record the fact that the value needs to be replicated and replay the write when the node becomes available. Hinted handoff is enabled by default. The following table describes how hinted handoff can be configured.

<table class="table">

  <tr>
    <th>Property</th>
    <th>Type</th>
    <th>Description</th>
  </tr>
  
   <tr>
    <td>hinted_handoff_enabled</td>
    <td>boolean</td>
    <td>If true, hinted handoff will be used to maintain consistency during node failure.</td>
  </tr>

<tr>
    <td>max_hint_window_in_ms</td>
    <td>integer</td>
    <td>The maximum amount of time, in ms, that Cassandra will record hints for an unavailable node.</td>
  </tr>
  
  <tr>
    <td>max_hint_delivery_threads</td>
    <td>integer</td>
    <td>The number of threads that deliver hints. The default value of 2 should be sufficient most use cases.</td>
  </tr>

</table>

### Dynamic Snitch Configuration

The endpoint snitch for the service is always the `GossipPropertyFileSnitch`, but, in addition to this, Cassandra uses a dynamic snitch to determine when requests should be routed away from poorly performing nodes. The configuration parameters below control the behavior of the snitch.

<table class="table">

  <tr>
    <th>Property</th>
    <th>Type</th>
    <th>Description</th>
  </tr>
  
   <tr>
    <td>dynamic_snitch_badness_threshold</td>
    <td>number</td>
    <td>Controls how much worse a poorly performing node has to be before the dynamic snitch prefers other replicas over it. A value of 0.2 means Cassandra continues to prefer the static snitch values until the node response time is 20% worse than the best performing node. Until the threshold is reached, incoming requests are statically routed to the closest replica.</td>
  </tr>
  
  <tr>
    <td>dynamic_snitch_reset_interval_in_ms</td>
    <td>integer</td>
    <td>Time interval, in ms, to reset all node scores, allowing a bad node to recover.</td>
  </tr>
  
   <tr>
    <td>dynamic_snitch_update_interval_in_ms</td>
    <td>integer</td>
    <td>The time interval, in ms, for node score calculation. This is a CPU-intensive operation. Reduce this interval with caution.</td>
  </tr>

</table>

### Global Key Cache Configuration

The partition key cache is a cache of the partition index for a Cassandra table. It is enabled by setting the parameter when creating the table. Using the key cache instead of relying on the OS page cache can decrease CPU and memory utilization. However, as the value associated with keys in the partition index is not cached along with key, reads that utilize the key cache will still require that row values be read from storage media, through the OS page cache. The following configuraiton items control the system global configuration for key caches.

<table class="table">

  <tr>
    <th>Property</th>
    <th>Type</th>
    <th>Description</th>
  </tr>
  
   <tr>
    <td>key_cache_save_period</td>
    <td>integer</td>
    <td>The duration in seconds that keys are saved in cache. Saved caches greatly improve cold-start speeds and has relatively little effect on I/O.</td>
  </tr>
  
  <tr>
    <td>key_cache_size_in_mb</td>
    <td>integer</td>
    <td>The maximum size of the key cache in Mb. When no value is set, the cache is set to the smaller of 5% of the available heap, or 100MB. To disable set to 0.</td>
  </tr>

</table>

### Global Row Cache Configuration

Row caching caches both the key and the associated row in memory. During the read path, when rows are reconstructed from the `MemTable` and `SSTables`, the reconstructed row is cached in memory preventing further reads from storage media until the row is ejected or dirtied.
Like key caching, row caching is configurable on a per table basis, but it should be used with extreme caution. Misusing row caching can overwhelm the JVM and cause Cassandra to crash. Use row caching under the following conditions: only if you are absolutely certain that doing so will not overwhelm the JVM, the partition you will cache is small, and client applications will read most of the partition all at once.

The following configuration properties are global for all row caches.

<table class="table">

  <tr>
    <th>Property</th>
    <th>Type</th>
    <th>Description</th>
  </tr>
  
   <tr>
    <td>row_cache_save_period</td>
    <td>integer</td>
    <td>The duration in seconds that rows are saved in cache. Saved caches greatly improve cold-start speeds and has relatively little effect on I/O.</td>
  </tr>
  
  <tr>
    <td>row_cache_size_in_mb</td>
    <td>integer</td>
    <td>The maximum size of the key cache in Mb. Make sure to provide enough space to contain all the rows for tables that will have row caching enabled.</td>
  </tr>

</table>

## Operating System Configuration
In order for Cassandra to function correctly there are several important configuration modifications that need to be performed to the OS hosting the deployment.
#### Time Synchronization
While writes in Cassandra are atomic at the column level, Cassandra only implements last write wins ordering to resolve consistency during concurrent writes to the same column. If system time is not synchronized across DOCS agent nodes, this will result in inconsistencies in the value stored with respect to the ordering of mutations as observed by the client. It is imperative that a mechanism, such as NTP, is used for time synchronization.
#### Configuration Settings
In addition to time synchronization, Cassandra requires OS level configuration settings typical of a production data base server.

<table class="table">

  <tr>
    <th>File</th>
    <th>Setting</th>
    <th>Value</th>
    <th>Reason</th>
  </tr>
  
   <tr>
    <td>/etc/sysctl.conf</td>
    <td>vm.max_map_count</td>
    <td>131702</td>
    <td>Aside from calls to the system malloc implementation, Cassandra uses mmap directly to memory map files. Exceeding the number of allowed memory mappings will cause a Cassandra node to fail.</td>
  </tr>
  
   <tr>
    <td>/etc/sysctl.conf</td>
    <td>vm.swappiness</td>
    <td>0</td>
    <td>If the OS swaps out the Cassandra process, it can fail to respond to requests, resulting in the node being marked down by the cluster.</td>
  </tr>
  
  <tr>
    <td>/etc/security/limits.conf</td>
    <td>memlock</td>
    <td>unlimited</td>
    <td>A Cassandra node can fail to load and map SSTables due to insufficient memory mappings into process address space. This will cause the node to terminate.</td>
  </tr>
  
  <tr>
    <td>/etc/security/limits.conf</td>
    <td>nofile</td>
    <td>unlimited</td>
    <td>If this value is too low, a Cassandra node will terminate due to insufficient file handles.</td>
  </tr>
  
  <tr>
    <td>/etc/security/limits.conf, /etc/security/limits.d/90-nproc.conf</td>
    <td>nproc</td>
    <td>32768</td>
    <td>A Cassandra node spawns many threads, which go towards kernel nproc count. If nproc is not set appropriately, the node will be killed.</td>
  </tr>

</table>

## Connecting Clients

The only supported client for the DSOC Cassandra Service is the Datastax Java CQL Driver. Note that this means that Thrift RPC-based clients are not supported for use with this service and any legacy applications that use this communication mechanism are run at the user's risk.

### Connection Info Using the CLI

The following command can be executed from the cli to retrieve a set of nodes to connect to.

```
dcos cassandra --name=<service-name> connection
```

### Connection Info Response

The response is as below.

```
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

```
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

# Managing

## Add a Node
Increase the `NODES` value via Marathon as described in the [Configuration Update](#configuration-update) section. This creates an update plan as described in that section. An additional node will be added as the last block of that plan. After a node has been added, you should run cleanup, as described in the [Cleanup](#cleanup) section. It is safe to delay running cleanup until off-peak hours.

### Node Status

It is sometimes useful to retrieve information about a Cassandra node for troubleshooting or to examine the node's properties. Use the following CLI command to request that a node report its status:

```
$ dcos cassandra --name=<service-name> node status <nodeid>
```

This command queries the node status directly from the node. If the command fails to return, it may indicate that the node is troubled. Here, `nodeid` is the the sequential integer identifier of the node (e.g. 0, 1, 2 , ..., n).

Result:

```
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

<table class="table">

  <tr>
    <th>Property</th>
    <th>Type</th>
    <th>Description</th>
  </tr>
  
   <tr>
    <td>data_center</td>
    <td>string</td>
    <td>The datacenter that the Cassandra node is configured to run in. This has implications for tables created with topology-aware replication strategies. Multidatacenter topologies are not currently supported, but they will be in future releases.</td>
  </tr>
  
  <tr>
    <td>endpoint</td>
    <td>string</td>
    <td>The address of the storage service in the cluster.</td>
  </tr>
  
  <tr>
    <td>gossip_initialized</td>
    <td>boolean</td>
    <td>If true, the node has initialized the internal gossip protocol. This value should be true if the node is healthy.</td>
  </tr>
  
  <tr>
    <td>gossip_running</td>
    <td>boolean</td>
    <td>If true, the node's gossip protocol is running. This value should be true if the node is healthy.</td>
  </tr>
  
  <tr>
    <td>host_id</td>
    <td>string</td>
    <td>The host id that is exposed to as part of Cassandra's internal protocols. This value may be useful when examining the Cassandra process logs in the cluster. Hosts are sometimes referenced by id.</td>
  </tr>
  
  <tr>
    <td>joined</td>
    <td>true</td>
    <td>If true, the node has successfully joined the cluster. This value should always be true if the node is healthy.</td>
  </tr>
 
 <tr>
    <td>mode</td>
    <td>string</td>
    <td>The operating mode of the Cassandra node. If the mode is STARTING, JOINING, or JOINED, the node is initializing. If the mode is NORMAL, the node is healthy and ready to receive client requests.</td>
  </tr>
  
  <tr>
    <td>native_transport_running</td>
    <td>boolean</td>
    <td>If true, the node can service requests over the native transport port using the CQL protocol. If this value is not true, then the node is not capable of serving requests to clients.</td>
  </tr>
  
  <tr>
    <td>rack</td>
    <td>string</td>
    <td>The rack assigned to the Cassandra node. This property is important for topology-aware replication strategies. For the DCOS Cassandra service all nodes in the cluster should report the same value.</td>
  </tr>
  
  <tr>
    <td>token_count</td>
    <td>integer</td>
    <td>The number of tokens assigned to the node. The Cassandra DCOS service only supports virtual node-based deployments. Because the resources allocated to each instance are homogenous, the number of tokens assigned to each node is identical and should always be 256.</td>
  </tr>
  
   <tr>
    <td>version</td>
    <td>string</td>
    <td>The version of Cassandra that is running on the node.</td>
  </tr>
 
</table>

### Node Info

To view general information about a node, the following command my be run from the CLI.
```
$ dcos cassandra --name=<service-name> node describe <nodeid>
```
In contrast to the status command, this command requests information from the DCOS Cassandra Service and not the Cassandra node.

In contrast to the `status` command, `node describe` requests information from the DCOS Cassandra Service and not the Cassandra node. 

Result:

```
{
    "hostname": "10.0.3.64",
    "id": "node-0_2db151cb-e837-4fef-b17b-fbdcd25fadcc",
    "name": "node-0",
    "mode": "NORMAL",
    "slave_id": "8a22d1e9-bfc6-4969-a6bf-6d54c9d41ee3-S4",
    "state": "TASK_RUNNING"
}
```

<table class="table">

  <tr>
    <th>Property</th>
    <th>Type</th>
    <th>Description</th>
  </tr>
  
   <tr>
    <td>hostname</td>
    <td>string</td>
    <td>The hostname or IP address of the DCOS agent on which the node is running.</td>
  </tr>
  
  <tr>
    <td>id</td>
    <td>string</td>
    <td>The DCOS identifier of the task for the Cassandra node.</td>
  </tr>
  
   <tr>
    <td>name</td>
    <td>string</td>
    <td>The name of the Cassandra node.</td>
  </tr>
  
  <tr>
    <td>mode</td>
    <td>string</td>
    <td>The operating mode of the Cassandra node as recorded by the DCOS Cassandra service. This value should be eventually consistent with the mode returned by the status command.</td>
  </tr>
  
   <tr>
    <td>slave_id</td>
    <td>string</td>
    <td>The identifier of the DCOS slave agent where the node is running.</td>
  </tr>
  
  <tr>
    <td>state</td>
    <td>string</td>
    <td>The state of the task for the Cassandra node. If the node is being installed, this value may be TASK_STATING or TASK_STARTING. Under normal operating conditions the state should be TASK_RUNNING. The state may be temporarily displayed as TASK_FINISHED during configuration updates or upgrades.</td>
  </tr>
 
</table>

## Cleanup

When nodes are added or removed from the ring, a node can lose part of its partition range. Cassandra does not automatically remove data when this happens. You can tube cleanup to remove the unnecessary data.

Cleanup can be a CPU- and disk-intensive operation, so you may want to delay running cleanup until off-peak hours. The DCOS Cassandra service will minimize the aggregate CPU and disk utilization for the cluster by performing cleanup for each selected node sequentially.

To perform a cleanup from the CLI, enter the following command:

```
$ dcos cassandra --name=<service-name> cleanup --nodes=<nodes> --key_spaces=<key_spaces> --column_families=<column_families>
```

Here, `<nodes>` is an optional comma-separated list indicating the nodes to cleanup, `<key_spaces>` is an optional comma-separated list of the key spaces to cleanup, and `<column-families>` is an optional comma-separated list of the column-families to cleanup.
If no arguments are specified a cleanup will be performed for all nodes, key spaces, and column families.

## Repair
Over time the replicas stored in a Cassandra cluster may become out of sync. In Cassandra, hinted handoff and read repair maintain the consistency of replicas when a node is temporarily down and during the data read path. However, as part of regular cluster maintenance, or when a node is replaced, removed, or added, manual anti-entropy repair should be performed. 
Like cleanup, repair can be a CPU and disk intensive operation. When possible, it should be run during off peak hours. To minimize the impact on the cluster, the DCOS Cassandra Service will run a sequential, primary range, repair on each node of the cluster for the selected nodes, key spaces, and column families.

To perform a repair from the CLI, enter the following command:

```
$ dcos cassandra --name=<service-name> repair --nodes=<nodes> --key_spaces=<key_spaces> --column_families=<column_families>
```

Here, `<nodes>` is an optional comma-separated list indicating the nodes to repair, `<key_spaces>` is an optional comma-separated list of the key spaces to repair, and `<column-families>` is an optional comma-separated list of the column-families to repair.
If no arguments are specified a repair will be performed for all nodes, key spaces, and column families.
 
## Backup and Restore

DCOS Cassandra supports backup and restore from S3 storage for disaster recovery purposes.

Cassandra takes a snapshot your tables and ships them to a remote location. Once the snapshots have been uploaded to a remote location, you can restore the data to a new cluster, in the event of a disaster, or restore them to an existing cluster, in the event that a user error has caused a data loss.

### Backup

You can take a complete snapshot of your DCOS Cassandra ring and upload the artifacts to S3.

To perform a backup, enter the following command on the DCOS CLI:

```
$ dcos cassandra --name=<service-name> backup start \
    --backup_name=<backup-name> \
    --external_location=s3://<bucket-name> \
    --s3_access_key=<s3-access-key> \
    --s3_secret_key=<s3-secret-key>
```

Check status of the backup:

```
$ dcos cassandra --name=<service-name> backup status
```

### Restore

You can restore your DCOS Cassandra snapshots on a new Cassandra ring.

To restore, enter the following command on the DCOS CLI:

```
$ dcos cassandra --name=<service-name> restore start \
    --backup_name=<backup-name> \
    --external_location=s3://<bucket-name> \
    --s3_access_key=<s3-access-key> \
    --s3_secret_key=<s3-secret-key>
```

Check the status of the restore:

```
$ dcos cassandra --name=<service-name> restore status
```

# Troubleshooting

## Configuration Update Errors
The plan below shows shows contains a configuration error that will not allow the installation or configuration update to progress.

```
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

## Replacing a Permanently Failed Node
The DCOS Cassandra Service is resilient to temporary node failures. However, if a DCOS agent hosting a Cassandra node is permanently lost, manual intervention is required to replace the failed node. The following command should be used to replace the node residing on the failed server.

```
$ dcos cassandra --name=<service-name> node replace <node_id>
```

This will replace the node with a new node of the same name running on a different server. The new node will take over the token range owned by its predecessor. After replacing a failed node, you should run [Cleanup]

# API Reference
The DCOS Cassandra Service implements a REST API that may be accessed from outside the cluster. If the DCOS cluster is configured with OAuth enabled, then you must acquire a valid token and include that token in the Authorization header of all requests. The <auth_token> parameter below is used to represent this token. 
The <dcos_url> parameter referenced below indicates the base URL of the DCOS cluster on which the Cassandra Service is deployed. Depending on the transport layer security configuration of your deployment this may be a HTTP or a HTTPS URL.

## Configuration

### View the Installation Plan

```
$ curl -H "Authorization:token=<auth_token>" <dcos_url>/service/cassandra/v1/plan
```

### Retrieve Connection Info

```
$ curl -H "Authorization:token=<auth_token>" <dcos_url>/cassandra/v1/nodes/connect
```

You will see a response similar to the following:

```
{
    "nodes": [
        "10.0.0.47:9042",
        "10.0.0.50:9042",
        "10.0.0.49:9042"
    ]
}
```

This JSON array contains a list of valid nodes that the client can use to connect to the Cassandra cluster. For availability reasons, it is best to specify multiple nodes in the CQL Driver configuration used by the application.

### Pause Installation

The installation will pause after completing installation of the current node and wait for user input.

```
$ curl -X POST -H "Authorization:token=<auth_token>" <dcos_url>/service/cassandra/v1/plan?cmd=interrupt
```

### Resume Installation

The REST API request below will resume installation at the next pending node.

```
$ curl -X PUT <dcos_surl>/service/cassandra/v1/plan?cmd=proceed
```

## Managing

### Node Status
Retrieve the status of a node by sending a GET request to `/v1/nodes/<node-#>/status`:

```
$ curl  -H "Authorization:token=<auth_token>" <dcos_url>/service/cassandra/v1/nodes/<node-#>/status
```

### Node Info
Retrieve node information by sending a GET request to `/v1/nodes/<node-#>/info`:

```
$ curl  -H "Authorization:token=<auth_token>" <dcos_url>/service/cassandra/v1/nodes/</node-#>/info
```
### Cleanup

First, create the request payload, for example, in a file `cleanup.json`:

```
{
    "nodes":["*"],
    "key_spaces":["my_keyspace"],
    "column_families":["my_cf_1", "my_cf_w"]
}
```

In the above, the nodes list indicates the nodes on which cleanup will be performed. The value [*], indicates to perform the cleanup cluster wide. key_spaces and column_families indicate the key spaces and column families on which cleanup will be performed. These may be ommitted if all key spaces and/or all column families should be targeted. The json below shows the request payload for a cluster wide cleanup operation of all key spaces and column families.

```
{
    "nodes":["*"]
}
```

```
$ curl -X PUT -H "Content-Type:application/json" -H "Authorization:token=<auth_token>" <dcos_url>/service/cassandra/v1/cleanup/start --data @cleanup.json
```

### Repair

First, create the request payload, for example, in a file `repair.json`:

```
{
    "nodes":["*"],
    "key_spaces":["my_keyspace"],
    "column_families":["my_cf_1", "my_cf_w"]
}
```
In the above, the nodes list indicates the nodes on which the repair will be performed. The value [*], indicates to perform the repair cluster wide. key_spaces and column_families indicate the key spaces and column families on which repair will be performed. These may be ommitted if all key spaces and/or all column families should be targeted. The json below shows the request payload for a cluster wide repair operation of all key spaces and column families.

```
{
    "nodes":["*"]
}
```

```
curl -X PUT -H "Content-Type:application/json" -H "Authorization:token=<auth_token>" <dcos_url>/service/cassandra/v1/repair/start --data @repair.json
```

### Backup

First, create the request payload, for example, in a file `backup.json`:

```
{
    "backup_name":"<backup-name>",
    "external_location":"s3://<bucket-name>",
    "s3_access_key":"<s3-access-key>",
    "s3_secret_key":"<s3-secret-key>"
}
```

Then, submit the request payload via `PUT` request to `/v1/backup/start`

```
$ curl -X PUT -H "Content-Type: application/json" -H "Authorization:token=<auth_token>" -d @backup.json <dcos_url>/service/cassandra/v1/backup/start
{"status":"started", message:""}
```

Check status of the backup:

```
$ curl -X GET http://cassandra.marathon.mesos:9000/v1/backup/status
```

### Restore

First, bring up a new instance of your Cassandra cluster with the same number of nodes as the cluster whose snapshot backup you want to restore.

Next, create the request payload, for example, in a file `restore.json`:

```
{
    "backup_name":"<backup-name-to-restore>",
    "external_location":"s3://<bucket-name-where-backups-are-stored>",
    "s3_access_key":"<s3-access-key>",
    "s3_secret_key":"<s3-secret-key>"
}
```

Next, submit the request payload via `PUT` request to `/v1/restore/start`

```
$ curl -X PUT -H "Content-Type: application/json" -H "Authorization:token=<auth_token>" -d @restore.json <dcos_url>/service/cassandra/v1/restore/start
{"status":"started", message:""}
```

Check status of the restore:

```
$ curl -X -H "Authorization:token=<auth_token>" <dcos_url>/service/cassandra/v1/restore/status
```
#Multi-Data Center Deployments
##Requirements
- All nodes in each data center MUST be reachable by the configured network addresses.
- All DCOS Cassandra deployments participating in the cluster MUST be configured to belong to the same cluster.
- Each DCOS Cassandra deployment participating in the cluster MUST be configured to belong to different data center's.

##Installing the Initial Data Center

Installation of the initial cluster proceeds as indicated in the [Installation](#installation) section. If all virtual data centers in the Cassandra cluster will reside in the same DCOS cluster, no additional configuration is necessary. If, however, the cluster will span multiple DCOS clusters the `service.data_center_url` property of the cluster must be set to an address that is reachable and resolvable by the nodes in all subsequently installed deployments. Depending on the configuration of your DCOS deployment, a good choice for this value may be the service router endpoint for the DCOS Cassandra Service (i.e. <dcos_url>/service/cassandra/v1/datacenter>). If all data centers in the cluster reside in the same DCOS cluster, the `service.data_center_url` will automatically be set to the MesosDNS address of the cluster (i.e. http://<service.name>.marathon.mesos:<api_port>/v1/datacenter) if left blank. For example, if the default service name and api port are used the URL is set to http://cassandra.marathon.mesos:9000/v1/datacenter.

##Installing Additional Data Centers
Prior to installing additional data centers, you MUST wait until at least one node per data center is active, and you SHOULD wait until at least the number of configured seed nodes is active.

Installation of a second data center requires that `service.external_dcs` parameter be set to include the values set in the `servie.data_center_url` property of all prior installations for the cluster. If you have installed one data center than only this should be included. If you have installed two data centers, then the URLs of both should be included as a comma separated list. Note that the `service.cluster` property MUST be identical for all data centers in the same cluster, and the `service.data_center` MUST be unique for all data centers in the cluster. As with the initial installation, if the cluster will span multiple DCOS clusters, the `service.data_center_url` must be configured to a URL that is reachable and resolvable by the nodes in all previous deployments.
During the installation of additional data centers, you will see a plan generated as below.

```
{
	"phases": [{
		"id": "72b91214-ad7e-4450-92cd-cc841fe531ad",
		"name": "Reconciliation",
		"blocks": [{
			"id": "89771069-d209-4a96-8dce-badcb2bc1abb",
			"status": "Complete",
			"name": "Reconciliation",
			"message": "Reconciliation complete",
			"has_decision_point": false
		}],
		"status": "Complete"
	}, {
		"id": "9be9c790-dd2e-4258-8ea6-5a4efb8b4eb3",
		"name": "Sync DataCenter",
		"blocks": [{
			"id": "bc1d0f6b-d2da-4680-aa96-580d740c04e9",
			"status": "Complete",
			"name": "Sync DataCenter",
			"message": "Syncing data center @ http://cassandra.marathon.mesos:9000/v1/datacenter",
			"has_decision_point": false
		}],
		"status": "Complete"
	}, {
		"id": "c3d48cb5-cb5e-4885-821b-63371ab668ec",
		"name": "Deploy",
		"blocks": [{
			"id": "8a6e6799-c518-478c-a429-2b8215af4573",
			"status": "Complete",
			"name": "node-0",
			"message": "Deploying Cassandra node node-0",
			"has_decision_point": false
		}, {
			"id": "07a3800e-ddaf-4f56-ac36-d607ca9fc46b",
			"status": "Complete",
			"name": "node-1",
			"message": "Deploying Cassandra node node-1",
			"has_decision_point": false
		}, {
			"id": "91405f22-fbe8-40c3-b236-8a8725d746bf",
			"status": "Complete",
			"name": "node-2",
			"message": "Deploying Cassandra node node-2",
			"has_decision_point": false
		}],
		"status": "Complete"
	}],
	"errors": [],
	"status": "Complete"
}
```
In the above, the Sync DataCenters phase will contain a Block for each data center that 
contains a DCOS Cassandra service deployment containing a partition of the cluster. During the execution of each Block, the DCOS Cassandra Service will register its
local endpoint with the data center indicated by the URL in the Block's message, and, it 
will retrieve the current seed nodes for that data center. 
Once the Installation plan progresses to deployment, it will provide the seeds from the 
external data centers to the nodes it deploys, allowing the cluster to span multiple 
datacenters.
After the Sync DataCenter Block completes, both data centers will periodically poll each 
other for modifications to the seed set.

# Limitations

- Cluster backup and restore can only be performed sequentially across the entire data center. While this makes cluster backup and restore time consuming, it also ensures that taking backups and restoring them will not overwhelm the cluster or the network. In the future, DCOS Cassandra could allow for a user-specified degree of parallelism when taking backups.
- Cluster restore can only restore a cluster of the same size as, or larger than, the cluster from which the backup was taken.
- While nodes can be replaced, there is currently no way to shrink the size of the cluster. Future releases will contain decommissions and remove operations.
- Anti-entropy repair can only be performed sequentially, for the primary range of each node, across and entire data center. There are use cases where one might wish to repair an individual node, but running the repair procedure as implemented is always sufficient to repair the cluster.
- Once a cluster is configured to span multiple data centers, there is no way to shrink 
the cluster back to a single data center.
