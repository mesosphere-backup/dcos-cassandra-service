---
post_title: Managing
menu_order: 70
feature_maturity: preview
enterprise: 'no'
---

# Manage Nodes

## Add a Node
Increase the `NODES` value from the DC/OS dashboard as described in the Configuration Update section. This creates an update plan as described in that section. An additional node will be added as the last step of that plan. After a node has been added, you should run cleanup, as described in the [Cleanup](#cleanup) section of this page. It is safe to delay running cleanup until off-peak hours.

## Node Status

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
    <td>The rack assigned to the Cassandra node. This property is important for topology-aware replication strategies. For the DC/OS Apache Cassandra service all nodes in the cluster should report the same value.</td>
  </tr>

  <tr>
    <td>token_count</td>
    <td>integer</td>
    <td>The number of tokens assigned to the node. The Cassandra DC/OS service only supports virtual node-based deployments. Because the resources allocated to each instance are homogenous, the number of tokens assigned to each node is identical and should always be 256.</td>
  </tr>

   <tr>
    <td>version</td>
    <td>string</td>
    <td>The version of Cassandra that is running on the node.</td>
  </tr>

</table>

## Node Info

To view general information about a node, run the following command from the CLI.
```
$ dcos cassandra --name=<service-name> node describe <nodeid>
```

In contrast to the `status` command, `node describe` requests information from the DC/OS Apache Cassandra Service and not the Cassandra node.

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
    <td>The hostname or IP address of the DC/OS agent on which the node is running.</td>
  </tr>

  <tr>
    <td>id</td>
    <td>string</td>
    <td>The DC/OS identifier of the task for the Cassandra node.</td>
  </tr>

   <tr>
    <td>name</td>
    <td>string</td>
    <td>The name of the Cassandra node.</td>
  </tr>

  <tr>
    <td>mode</td>
    <td>string</td>
    <td>The operating mode of the Cassandra node as recorded by the DC/OS Apache Cassandra service. This value should be eventually consistent with the mode returned by the status command.</td>
  </tr>

   <tr>
    <td>slave_id</td>
    <td>string</td>
    <td>The identifier of the DC/OS slave agent where the node is running.</td>
  </tr>

  <tr>
    <td>state</td>
    <td>string</td>
    <td>The state of the task for the Cassandra node. If the node is being installed, this value may be TASK_STATING or TASK_STARTING. Under normal operating conditions the state should be TASK_RUNNING. The state may be temporarily displayed as TASK_FINISHED during configuration updates or upgrades.</td>
  </tr>

</table>

# Maintenance
Cassandra supports several maintenance operations including Cleanup, Repair, Backup, and Restore.  In general, attempting to run multiple maintenance operations simultaneously (e.g. Repair and Backup) against a single cluster is not recommended. Likewise, running maintenance operations against multiple Cassandra clusters linked in a multi-datacenter configuration is not recommended.

<a name="cleanup"></a>
## Cleanup

When nodes are added or removed from the ring, a node can lose part of its partition range. Cassandra does not automatically remove data when this happens. You can tube cleanup to remove the unnecessary data.

Cleanup can be a CPU- and disk-intensive operation, so you may want to delay running cleanup until off-peak hours. The DC/OS Apache Cassandra service will minimize the aggregate CPU and disk utilization for the cluster by performing cleanup for each selected node sequentially.

To perform a cleanup from the CLI, enter the following command:

```
$ dcos cassandra --name=<service-name> cleanup start --nodes=<nodes> --key_spaces=<key_spaces> --column_families=<column_families>
```

Here, `<nodes>` is an optional comma-separated list indicating the nodes to cleanup, `<key_spaces>` is an optional comma-separated list of the key spaces to cleanup, and `<column-families>` is an optional comma-separated list of the column-families to cleanup.
If no arguments are specified a cleanup will be performed for all nodes, key spaces, and column families.

To cancel a currently running cleanup from the CLI, enter the following command:

```
$ dcos cassandra --name=<service-name> cleanup stop
```

The operation will end after the current node has finished its cleanup.

## Repair
Over time the replicas stored in a Cassandra cluster may become out of sync. In Cassandra, hinted handoff and read repair maintain the consistency of replicas when a node is temporarily down and during the data read path. However, as part of regular cluster maintenance, or when a node is replaced, removed, or added, manual anti-entropy repair should be performed.

Like cleanup, repair can be a CPU and disk intensive operation. When possible, it should be run during off peak hours. To minimize the impact on the cluster, the DC/OS Apache Cassandra Service will run a sequential, primary range, repair on each node of the cluster for the selected nodes, key spaces, and column families.

To perform a repair from the CLI, enter the following command:

```
$ dcos cassandra --name=<service-name> repair start --nodes=<nodes> --key_spaces=<key_spaces> --column_families=<column_families>
```

Here, `<nodes>` is an optional comma-separated list indicating the nodes to repair, `<key_spaces>` is an optional comma-separated list of the key spaces to repair, and `<column-families>` is an optional comma-separated list of the column-families to repair.
If no arguments are specified a repair will be performed for all nodes, key spaces, and column families.

To cancel a currently running repair from the CLI, enter the following command:

```
$ dcos cassandra --name=<service-name> repair stop
```

The operation will end after the current node has finished its repair.
