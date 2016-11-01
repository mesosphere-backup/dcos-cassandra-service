---
post_title: Upgrade
menu_order: 30
---

1. In the DC/OS web interface, destroy the Cassandra instance to be updated. (This will not kill Cassandra node tasks).
1. Verify that you no longer see the Cassandra instance in the DC/OS web interface.
1. From the DC/OS CLI, install the latest version of Cassandra [with any customizations you require](1.7/usage/service-guides/cassandra/install-and-customize/) in a JSON options file:
    $ dcos package install cassandra --options=/path/to/options.json

   The command above will trigger the installation of the new Cassandra version. You can follow the upgrade progress by making a REST request identical to the one used to follow the progress of a [configuration upgrade](/1.8/usage/service-guides/cassandra/configuration/).

**Note:** The upgrade process will cause all of your Cassandra node processes to restart.

**If you are upgrading to or beyond 1.0.13-X.Y.Z of DC/OS Cassandra from an older version (pre `1.0.13-X.Y.Z`), here is the upgrade path:**

1. Perform a [backup operation](https://github.com/mesosphere/dcos-cassandra-service#backup) on your currently running Cassandra Service. Note the backup name and backup location.
1. [Install a new Cassandra service instance](https://github.com/mesosphere/dcos-cassandra-service#multiple-cassandra-cluster-installation).
1. Perform a [restore operation](https://github.com/mesosphere/dcos-cassandra-service#restore) on the new cluster created in Step #2.
1. When the restore operation is finished, verify that the data is restored correctly.
1. [Uninstall](https://github.com/mesosphere/dcos-cassandra-service#uninstall) the old cluster.
