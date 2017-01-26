---
post_title: Troubleshooting
menu_order: 90
feature_maturity: preview
enterprise: 'no'
---

# Configuration Update Errors
The plan below contains a configuration error that will not allow the installation or configuration update to progress.

```
{
    "errors": ["The number of seeds is greater than the number of nodes."],
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
    "status": "Error"
}
```
To proceed with the installation or configuration update, fix the indicated errors by updating the configuration as detailed in the Configuration Update section.

# Replacing a Permanently Failed Node
The DC/OS Apache Cassandra Service is resilient to temporary node failures. However, if a DC/OS agent hosting a Cassandra node is permanently lost, manual intervention is required to replace the failed node. The following command should be used to replace the node residing on the failed server. If you perform this action via the REST API, see the REST API Authentication part of the REST API Reference section for information on how this request must be authenticated.

Via CLI:
```
$ dcos cassandra --name=cassandra node replace 0
```

Via API:
```
$ curl -X PUT -H "Authorization: token=$AUTH_TOKEN" "<dcos_url>/service/cassandra/v1/nodes/replace?node=node-0"
```

This will replace the node with a new node of the same name running on a different server. The new node will take over the token range owned by its predecessor. After replacing a failed node, you should run Cleanup (See the Cleanup part of the Managing section for more information).

# Restarting a Node
To restart a given Cassandra node please use following. If you perform this action via the REST API, see REST API Authentication part of the REST API Reference section for information on how this request must be authenticated.

CLI:
```
$ dcos cassandra --name=cassandra node restart 0
```

API:
```
$ curl -X PUT -H "Authorization: token=$AUTH_TOKEN" "<dcos_url>/service/cassandra/v1/nodes/restart?node=node-0"
```

This will restart the node with the same name running on the same server.
