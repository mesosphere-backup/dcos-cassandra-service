---
post_title: Limitations
menu_order: 100
feature_maturity: preview
enterprise: 'no'
---


- Cluster backup and restore can only be performed sequentially across the entire datacenter. While this makes cluster backup and restore time consuming, it also ensures that taking backups and restoring them will not overwhelm the cluster or the network. In the future, DC/OS Apache Cassandra could allow for a user-specified degree of parallelism when taking backups.
- Cluster restore can only restore a cluster of the same size as, or larger than, the cluster from which the backup was taken.
- While nodes can be replaced, there is currently no way to shrink the size of the cluster. Future releases will contain decommissions and remove operations.
- Anti-entropy repair can only be performed sequentially, for the primary range of each node, across and entire datacenter. There are use cases where one might wish to repair an individual node, but running the repair procedure as implemented is always sufficient to repair the cluster.
- Once a cluster is configured to span multiple datacenters, there is no way to shrink
the cluster back to a single datacenter.
