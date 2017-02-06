---
post_title: Upgrade
menu_order: 20
feature_maturity: preview
enterprise: 'no'
---

# Overview

We support upgrade/rollback between adjacent versions only.  Concretely, to upgrade from version 2 to version 4, you must upgrade from 2 -> 3, then from 3 ->4.  

# Upgrade Instructions

1. In the DC/OS web interface, destroy the Cassandra instance to be updated. (This will not kill Cassandra node tasks).
1. Verify that you no longer see the Cassandra instance in the DC/OS web interface.
1. From the DC/OS CLI, install the N+1 version of Cassandra (where N is your current version) with any customizations you require in a JSON options file:
    
        $ dcos package install cassandra --options=/path/to/options.json

The command above will trigger the install of the new Cassandra version. You can follow the upgrade progress by making a REST request identical to the one used to follow the progress of a configuration upgrade. See the Configuring section for more information.

**Note:** The upgrade process will cause all of your Cassandra node
processes to restart.

# Specific Version Instructions

**If you are planning to upgrade to DC/OS Apache Cassandra 1.0.23 on Ubuntu or CentOS,
you must upgrade to 1.0.24 instead.  Do not upgrade to 1.0.23, which may not
work on those OSes.**

**If you are upgrading specifically from DC/OS Apache Cassandra 1.0.18, you must
upgrade to 1.0.20 or higher.  Do not upgrade to 1.0.19; this version is no longer 
supported and will be deleted in the near future. (See
https://github.com/mesosphere/dcos-cassandra-service/pull/346 for details)**

**If you are upgrading from DC/OS Apache Cassandra 1.0.19, you must
  upgrade to 1.0.21 or higher.  Do not upgrade to 1.0.20 (See
  https://github.com/mesosphere/dcos-cassandra-service/pull/346 for details)**

**If you are upgrading to 1.0.13-X.Y.Z of DC/OS Apache
  Cassandra from an older version (pre `1.0.13-X.Y.Z`), here is the
  upgrade path:**

1. Perform the backup operation on your currently running Cassandra Service. Note the backup name and backup location. See the Backup and Restore section for more information.
1. Install a new Cassandra Service instance. See the Install and Customize section for more information.
1. Perform the restore operation on the new cluster created in Step #2. See the Backup and Restore section for more information.
1. Once the restore operation is finished, check if the data is restored correctly.
1. Uninstall the old cluster. See the Uninstall section for more information.
