---
post_title: Install and Customize
menu_order: 0
feature_maturity: preview
enterprise: 'no'
---

Cassandra for DC/OS is available in the Universe and can be installed by using either the web interface or the DC/OS CLI.

**Prerequisites:**

-  [DC/OS and DC/OS CLI installed](https://docs.mesosphere.com/1.9/installing/) with a minimum of three agent nodes with eight GB of memory and ten GB of disk available on each agent.
-  Depending on your [security mode](https://docs.mesosphere.com/1.9/overview/security/security-modes/), Kafka requires a service authentication for access to DC/OS. For more information, see [Configuring DC/OS Access for Kafka](https://docs.mesosphere.com/service-docs/kafka/kafka-auth/).

   | Security mode | Service Account |
   |---------------|-----------------------|
   | Disabled      | Not available   |
   | Permissive    | Optional   |
   | Strict        | Required |
- Ports 7000, 7001, 7199, 9042, and 9160 must be available.

# Default Installation
The default installation may not be sufficient for a production deployment, but all cluster operations will work. If you are planning a production deployment with three replicas of each value and local quorum consistency for read and write operations (a very common use case), this configuration is sufficient for development and testing purposes and it may be scaled to a production deployment.

Install the Cassandra package. This may take a few minutes.

```bash
dcos package install cassandra
```

This command creates a new Cassandra cluster with three nodes. Two clusters cannot share the same name, so installing additional clusters beyond the default cluster requires customizing the `name` at install time for each additional instance. See the Custom Installation section for more information.

If you have more than one Cassandra cluster, use the `--name` argument after install time to specify which Cassandra instance to query. All `dcos cassandra` CLI commands accept the `--name` argument. If you do not specify a service name, the CLI assumes the default value, `cassandra`.

**Note:** You can also install Cassandra from the Universe > Packages tab of the DC/OS web interface. If you install Cassandra from the web interface, you must install the Cassandra DC/OS CLI subcommands separately. From the DC/OS CLI, enter:

```
dcos package install cassandra --cli
```

# Custom Installation

If you are ready to ship into production, you need to customize the deployment to suit the workload requirements of your applications. Customize the default deployment by creating an options JSON file, then pass it to `dcos package install` using the `--options` parameter.


Create a JSON options file named `sample-cassandra.json`:

```json
{
    "nodes": {
        "count": 10,
        "seeds": 3
    }
}
```

Install Cassandra with the configuration specified in the `sample-cassandra.json` file:

```bash
dcos package install --options=sample-cassandra.json cassandra
```

This cluster will have ten nodes and three seeds, instead of the default values of three nodes and two seeds. See the [Configuration Options](#configuration-options) section for a list of fields that can be customized via an options JSON file when the Cassandra cluster is created.

# Minimal Installation

**Note:** This configuration will not support replication of any kind, but it may be sufficient for early stage evaluation and development.

To start a minimal cluster with a single node, create a JSON options file name `sample-cassandra-minimal.json`:

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
       "mem": 128
    }
}
```

This will create a single node cluster with two GB of memory and 4 Gb of disk. You will need an additional 512 Mb for the DC/OS Apache Cassandra Service executor and 128 Mb for clusters tasks. The DC/OS Apache Cassandra Service scheduler needs 512 MB to run, but it does not need to be deployed on the same host as the node.

Install Cassandra with the configuration specified in the `sample-cassandra-minimal.json` file:

```bash
dcos package install --options=sample-cassandra-minimal.json cassandra
```

# Multiple Cassandra Cluster Installation

To install multiple Cassandra clusters, create a JSON options file named `sample-cassandra-multiple.json` and specify a unique `name` for each installation:

```json
{
   "service": {
       "name": "cassandra1"
   }
}
```

Install Cassandra with the configuration specified in the `sample-cassandra-multiple.json` file:

```bash
dcos package install cassandra --options=sample-cassandra-multiple.json
```

In order to avoid port conflicts, by default you cannot collocate more than one Cassandra instance on the same node.

# Installation Plan

When the DC/OS Cassandra service is initially installed it will generate an installation plan as shown below. You can view, pause, and resume installation via the REST API. See the REST API authentication section of the REST API Reference for information on how this request must be authenticated.

```json
{
    "errors": [],
    "phases": [
        {
            "steps": [
                {
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
	         "steps": [],
	         "status": "Complete"
        },
        {
            "steps": [
                {
                    "id": "440485ec-eba2-48a3-9237-b0989dbe9f68",
                    "message": "Deploying Cassandra node node-0",
                    "name": "node-0",
                    "status": "Complete"
                },
                {
                    "id": "84251eb9-218c-4700-a03c-50018b90d5a8",
                    "message": "Deploying Cassandra node node-1",
                    "name": "node-1",
                    "status": "InProgress"
                },
                {
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

## Viewing the Installation Plan
The plan can be viewed from the API via the REST endpoint. A curl example is provided below.

```
curl -H "Authorization: token=$AUTH_TOKEN" http://<dcos_url>/service/cassandra/v1/plan
```

## Plan Errors
If there are any errors that prevent installation, these errors are dispayed in the errors list. The presence of errors indicates that the installation cannot progress. See the Troubleshooting section for information on resolving errors.

## Reconciliation Phase
The first phase of the installation plan is the reconciliation phase. This phase ensures that the DC/OS Apache Cassandra service maintains the correct status for the Cassandra nodes that it has deployed. Reconciliation is a normal operation of the DC/OS Apache Cassandra Service and occurs each time the service starts. See [the Mesos documentation](http://mesos.apache.org/documentation/latest/reconciliation) for more information.

## Deploy Phase
The second phase of the installation is the deploy phase. This phase will deploy the requested number of Cassandra nodes. Each step in the phase represents an individual Cassandra node. In the plan shown above the first node, node-0, has been deployed, the second node, node-1, is in the process of being deployed, and the third node, node-2, is pending deployment based on the completion of node-1.

## Pausing Installation
In order to pause installation, issue a REST API request as shown below. The installation will pause after completing installation of the current node and wait for user input.

```
curl -X POST -H "Authorization: token=$AUTH_TOKEN" http:/<dcos_url>/service/cassandra/v1/plan/interrupt
```


## Resuming Installation
If the installation has been paused, the REST API request below will resume installation at the next pending node.

```
curl -X POST -H "Authorization: token=$AUTH_TOKEN" http://<dcos_url>/service/cassandra/v1/plan/continue
```

 [5]: https://github.com/mesosphere/dcos-vagrant
 [7]: http://mesos.apache.org/documentation/latest/reconciliation
