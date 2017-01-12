---
post_title: Multi-Datacenter Deployments
menu_order: 40
feature_maturity: preview
enterprise: 'no'
---

# Requirements
- All nodes in each datacenter MUST be reachable by the configured network addresses.
- All DC/OS Apache Cassandra deployments participating in the cluster MUST be configured to belong to the same cluster.
- Each DC/OS Apache Cassandra deployment participating in the cluster MUST be configured to belong to different datacenters.

# Installing the Initial Datacenter

Install the cluster as described in the Install and Customize section. If all virtual datacenters in the Cassandra cluster will reside in the same DC/OS cluster, no additional configuration is necessary.

The `service.data_center_url` will automatically be set to the Mesos DNS address of the cluster (i.e., `http://<service.name>.marathon.mesos:<api_port>/v1/datacenter`) if left blank. For example, if the default service name and api port are used, the URL is set to `http://cassandra.marathon.mesos:9000/v1/datacenter`.

If your Cassandra cluster spans multiple DC/OS clusters, perform the following steps to allow the Cassandra clusters to communicate with one another. For each of your datacenters:

1. Set up a reverse proxy, accessible from other datacenters, that routes to the Mesos DNS address of the DC/OS cluster.

1. Set that Mesos DNS address as your `service.data_center_url` and provide it to the other datacenters in the `service.external_data_center_urls` list.

# Installing Additional Datacenters
Prior to installing additional datacenters, you MUST wait until at least one node per datacenter is active, and you SHOULD wait until at least the number of configured seed nodes is active.

To install a second datacenter, you must set the `service.external_data_center_urls` to include the values set in the `service.data_center_url` property of all prior installations for the cluster. If you have installed one datacenter, then only this should be included. If you have installed two datacenters, include the URLs of both as a comma-separated list.

The `cassandra.cluster_name` property MUST be identical for all datacenters in the same cluster and the `service.data_center` MUST be unique for all datacenters in the cluster. As with the initial installation, if the cluster will span multiple DC/OS clusters, the `service.data_center_url` must be configured to a URL that is reachable and resolvable by the nodes in all previous deployments.

During the installation of additional datacenters, you will see a plan generated as below.

```
{
	"phases": [{
		"id": "72b91214-ad7e-4450-92cd-cc841fe531ad",
		"name": "Reconciliation",
		"steps": [{
			"id": "89771069-d209-4a96-8dce-badcb2bc1abb",
			"status": "Complete",
			"name": "Reconciliation",
			"message": "Reconciliation complete",
		}],
		"status": "Complete"
	}, {
		"id": "9be9c790-dd2e-4258-8ea6-5a4efb8b4eb3",
		"name": "Sync DataCenter",
		"steps": [{
			"id": "bc1d0f6b-d2da-4680-aa96-580d740c04e9",
			"status": "Complete",
			"name": "Sync DataCenter",
			"message": "Syncing data center @ http://cassandra.marathon.mesos:9000/v1/datacenter",
		}],
		"status": "Complete"
	}, {
		"id": "c3d48cb5-cb5e-4885-821b-63371ab668ec",
		"name": "Deploy",
		"steps": [{
			"id": "8a6e6799-c518-478c-a429-2b8215af4573",
			"status": "Complete",
			"name": "node-0",
			"message": "Deploying Cassandra node node-0",
		}, {
			"id": "07a3800e-ddaf-4f56-ac36-d607ca9fc46b",
			"status": "Complete",
			"name": "node-1",
			"message": "Deploying Cassandra node node-1",
		}, {
			"id": "91405f22-fbe8-40c3-b236-8a8725d746bf",
			"status": "Complete",
			"name": "node-2",
			"message": "Deploying Cassandra node node-2",
		}],
		"status": "Complete"
	}],
	"errors": [],
	"status": "Complete"
}
```

In the above, the `Sync DataCenter` phase has a step for each datacenter with a DC/OS Apache Cassandra service deployment that contains a partition of the cluster.

During the execution of each step the DC/OS Apache Cassandra Service registers its
local endpoint with the datacenter indicated by the URL in the step's message. Then, it retrieves the current seed nodes for that datacenter.

When the installation plan progresses to the `Deploy` phase, it provides the seeds from the
external datacenters to the nodes it deploys, allowing the cluster to span multiple
datacenters.

After the `Sync DataCenter` step completes, both datacenters will periodically poll each
other for modifications to the seed set.
