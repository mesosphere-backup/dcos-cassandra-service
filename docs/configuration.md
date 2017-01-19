---
post_title: Configuring
menu_order: 50
feature_maturity: preview
enterprise: 'no'
---

# Changing Configuration at Runtime

You can customize your cluster in-place when it is up and running.

The Cassandra scheduler runs as a Marathon process and can be reconfigured by changing values for the service from the DC/OS dashboard. These are the general steps to follow:

1. Go to the DC/OS dashboard.
1. Click the **Services** tab, then the name of the Cassandra service to be updated.
1. Within the Cassandra instance details view, click the menu in the upper right, then choose **Edit**.
1. In the dialog that appears, click the **Environment** tab and update any field(s) to their desired value(s). For example, to increase the number of nodes, edit the value for `NODES`.
1. Click **REVIEW & RUN** to apply any changes and cleanly reload the Cassandra scheduler. The Cassandra cluster itself will persist across the change.

## Configuration Deployment Strategy

Configuration updates are rolled out through execution of Update Plans. You can configure the way these plans are executed.

### Configuration Update Plans
This configuration update strategy is analogous to the installation procedure above. If the configuration update is accepted, there will be no errors in the generated plan, and a rolling restart will be performed on all nodes to apply the updated configuration.

# Configuration Update

Make the REST request below to view the current plan. See REST API authentication of the REST API Reference section for information on how this request must be authenticated.

```
$ curl -H "Authorization: token=$AUTH_TOKEN" -v http://<dcos_url>/service/cassandra/v1/plan
```

The response will look similar to this:

```
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
                    "status": "Pending"
                },
                {
                    "id": "84251eb9-218c-4700-a03c-50018b90d5a8",
                    "message": "Deploying Cassandra node node-1",
                    "name": "node-1",
                    "status": "Pending"
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
            "status": "Pending"
        }
    ],
    "status": "InProgress"
}
```

If you want to interrupt a configuration update that is in progress, enter the `interrupt` command.

```
$ curl -X POST -H "Authorization: token=$AUTH_TOKEN" http:/<dcos_url>/service/cassandra/v1/plan/interrupt
```


If you query the plan again, the response will look like this (notice `status: "Waiting"`):

```
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
                    "status": "Pending"
                },
                {
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

**Note:** The interrupt command can’t stop a step that is `InProgress`, but it will stop the change on the subsequent steps.

Enter the `continue` command to resume the update process.

```
$ curl -X -H "Authorization: token=$AUTH_TOKEN" POST http://<dcos_url>/service/cassandra/v1/plan/continue
```

After you execute the continue operation, the plan will look like this:

```
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
	         "name": "Sync Datacenter",
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
                    "status": "Complete"
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
            "status": "Pending"
        }
    ],
    "status": "InProgress"
}
```

# Configuration Options

The following describes the most commonly used features of DC/OS Apache Cassandra and how to configure them via the DC/OS CLI and the DC/OS GUI. There are two methods of configuring a Cassandra cluster. The configuration may be specified using a JSON file during installation via the DC/OS command line (See the Install and Customize section) or via modification to the Service Scheduler’s DC/OS environment at runtime (See the Configuration Update section). Note that some configuration options may only be specified at installation time, but these generally relate only to the service’s registration and authentication with the DC/OS scheduler.

## Service Configuration

The service configuration object contains properties that MUST be specified during installation and CANNOT be modified after installation is in progress. Service configuration example:

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
    <td>The name of the Cassandra service installation. This must be unique for each DC/OS Apache Cassandra service instance deployed on a DC/OS cluster.</td>
  </tr>

  <tr>
    <td>cluster</td>
    <td>string</td>
    <td>The cluster that the Cassandra service installation belongs to. Multiple DC/OS Apache Cassandra service instances may belong to the same cluster.</td>
  </tr>

  <tr>
    <td>data_center</td>
    <td>string</td>
    <td>The identifier of the datacenter that the DC/OS Apache Cassandra service will deploy. This MUST be unique for deployments supporting multiple datacenters. This
    MAY be identical for multiple deployments on the same DC/OS cluster that support different clusters.</td>
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
    <td>placement_constraint</td>
    <td>string</td>
    <td>A Marathon-style placement constraint to be used when deploying the Cassandra nodes. For example, "rack_id:LIKE:rack_[1-5],hostname:UNLIKE:avoid_host_.*". See <a href="https://mesosphere.github.io/marathon/docs/constraints.html">Marathon documentation</a> for more details, but note that constraints must be of the form "field1:operator1[:value1],field2:operator2[:value2]" when used here. Changing this setting after initial deployment is experimental.</td>
  </tr>

  <tr>
    <td>secret</td>
    <td>string</td>
    <td>An optional path to the file containing the secret that the service will use to authenticate with the Mesos Master in the DC/OS cluster. This parameter is optional, and should be omitted unless the DC/OS deployment is specifically configured for authentication.</td>
  </tr>

   <tr>
      <td>cpus</td>
      <td>number</td>
      <td>The number of CPU shares allocated to the DC/OS Apache Cassandra Service scheduler. </td>
    </tr>

    <tr>
      <td>mem</td>
      <td>integer</td>
      <td>The amount of memory, in MB, allocated for the DC/OS Apache Cassandra Service scheduler. This MUST be larger than the allocated heap. 2 Gb is a good choice.</td>
    </tr>

    <tr>
      <td>heap</td>
      <td>integer</td>
      <td>The amount of heap, in MB, allocated for the DC/OS Apache Cassandra Service scheduler. 1 Gb is a minimum for production installations.</td>
    </tr>

    <tr>
      <td>api_port</td>
      <td>integer</td>
      <td>The port that the scheduler will accept API requests on.</td>
    </tr>

    <tr>
    <td>data_center_url</td>
    <td>string</td>
    <td>This specifies the URL that the DC/OS Apache Cassandra service instance will advertise to other instances in the cluster.
    If you are not configuring a multi datacenter deployment this should be omitted.
    If you are configuring a multiple datacenter deployment inside the same DC/OS cluster, this should be omitted.
    If you are configuring a multiple datacenter deployment inside diffrent DC/OS clusters, this value MUST be set to a URL that
    is reachable and resolvable by the DC/OS Apache Cassandra instances in the remote datacenters. A good choice for this value is the admin
    router URL (i.e. <dcos_url>/service/cassandra/v1/datacenter).  </td>
    </tr>
    <tr>
    <td>external_data_centers</td>
    <td>string</td>
    <td>This specifies the URLs of the external datacenters that contain a cluster the DC/OS Apache Cassandra service will join as a comma separated list.
    This value should only be included when your deploying a DC/OS Apache Cassandra service instance that will extend an existing cluster. Otherwise, this
    value should be omitted. If this value is specified, the URLs contained in the comma separated list MUST be resolvable and reachable from the deployed cluster.
    In practice, they should be identical to the values specified in data_center_url configuration parameter for the instance whose cluster will be extended.
    </td>
  </tr>

</table>

- **In the DC/OS CLI, options.json**: `name` = string (default: `cassandra`)

## Node Configuration

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
    <td>The maximum and minimum heap size used by the Cassandra process in MB. This value SHOULD be at least 2 GB, and it SHOULD be no larger than 80% of the allocated memory for the container. Specifying very large heaps, greater than 8 GB, is currently not a supported configuration.
    Note: The value of heap size should not be greater than the total memory configured for the container via <b>mem</b> param. If value of <b>mem</b> is greater than <b>heap.size</b> then Linux operating system will use the remaining memory, <b>mem</b> - <b>heap.size</b> for <a href="https://en.wikipedia.org/wiki/Page_cache">PageCache</a>
    </td>
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

## Executor Configuration
The executor configuration object allows you to modify the resources associated with the DC/OS Apache Cassandra Service's executor. These properties should not be modified unless you are trying to install a small cluster in a resource constrained environment.
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
      <td>The number of CPU shares allocated to the DC/OS Apache Cassandra Service executor. </td>
    </tr>

    <tr>
      <td>mem</td>
      <td>integer</td>
      <td>The amount of memory, in MB, allocated for the DC/OS Apache Cassandra Service scheduler. This MUST be larger than the allocated heap.</td>
    </tr>

    <tr>
      <td>heap</td>
      <td>integer</td>
      <td>The amount of heap, in MB, allocated for the DC/OS Apache Cassandra Service executor.</td>
    </tr>

    <tr>
      <td>disk</td>
      <td>integer</td>
      <td>The amount of disk, in MB, allocated for the DC/OS Apache Cassandra Service executor.</td>
    </tr>

    <tr>
      <td>api_port</td>
      <td>integer</td>
      <td>The port that the executor will accept API requests on.</td>
    </tr>

</table>

## Task Configuration
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
      <td>The number of CPU shares allocated to the DC/OS Apache Cassandra Service tasks. </td>
    </tr>
    <tr>
      <td>mem</td>
      <td>integer</td>
      <td>The amount of memory, in MB, allocated for the DC/OS Apache Cassandra Service tasks.</td>
    </tr>
</table>

## Cassandra Application Configuration

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

## Communication Configuration

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

## Commit Log Configuration

The DC/OS Apache Cassandra service only supports the commitlog_sync model for configuring the Cassandra commit log. In this model a node responds to write requests after writing the request to file system and replicating to the configured number of nodes, but prior to synchronizing the commit log file to storage media. Cassandra will synchronize the data to storage media after a configurable time period. If all nodes in the cluster should fail, at the Operating System level or below, during this window the acknowledged writes will be lost. Note that, even if the JVM crashes, the data will still be available on the nodes persistent volume when the service recovers the node.The configuration parameters below control the window in which data remains acknowledged but has not been written to storage media.

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

## Column Index Configuration

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

## Hinted Handoff Configuration

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

## Dynamic Snitch Configuration

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

## Global Key Cache Configuration

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

## Global Row Cache Configuration

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

## Authentication and Authorization Configuration

Authentication and authorization may be independently configured.  By default they are set to `AllowAllAuthenticator` and `AllowAllAuthorizer` respectively.  This is necessary to allow initial access to a new cluster.  After initial installation these values may be changed by changing the appropriate environment variables, `CASSANDRA_AUTHENTICATOR` and `CASSANDRA_AUTHORIZER`.
Further information regarding how to change authentication and authorization of a Cassaandra cluster is available [here](https://docs.datastax.com/en/cassandra/3.0/cassandra/configuration/secureConfigNativeAuth.html) and [here](https://docs.datastax.com/en/cassandra/3.0/cassandra/configuration/secureConfigInternalAuth.html).  The necessary changes to the cassandra.yml file referenced there are accomplished by changing the environment variables already mentioned.

<table class="table">

  <tr>
    <th>Property</th>
    <th>Type</th>
    <th>Description</th>
  </tr>

  <tr>
    <td>authenticator</td>
    <td>string</td>
    <td>The authentication backend. It implements IAuthenticator, which is used to identify users.</td>
  </tr>

   <tr>
    <td>authorizer</td>
    <td>string</td>
    <td>The authorization backend. It implements IAuthenticator, which limits access and provides permissions.</td>
  </tr>

</table>

# Operating System Configuration
In order for Cassandra to function correctly there are several important configuration modifications that need to be performed to the OS hosting the deployment.
## Time Synchronization
While writes in Cassandra are atomic at the column level, Cassandra only implements last write wins ordering to resolve consistency during concurrent writes to the same column. If system time is not synchronized across DOCS agent nodes, this will result in inconsistencies in the value stored with respect to the ordering of mutations as observed by the client. It is imperative that a mechanism, such as NTP, is used for time synchronization.
## Configuration Settings
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
    <td>1048575</td>
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
