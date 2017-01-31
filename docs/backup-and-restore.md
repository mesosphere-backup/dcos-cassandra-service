---
post_title: Backup and Restore
menu_order: 60
feature_maturity: preview
enterprise: 'no'
---

DC/OS Apache Cassandra supports backup and restore from S3 storage for disaster recovery purposes.

Cassandra takes a snapshot of your tables and ships them to a remote location. Once the snapshots have been uploaded to a remote location, you can restore the data to a new cluster, in the event of a disaster, or restore them to an existing cluster, in the event that a user error has caused a data loss.

**Note:** Backup and restore is not guaranteed to work across arbitrary versions of the Cassandra service. For example restoring a backup from a 1.0.16 cluster to a 1.0.21 cluster will not work. It is recommended that backups be restored to clusters of the same version from which they were created.

# Backup

You can take a complete snapshot of your DC/OS Apache Cassandra ring and upload the artifacts to S3 or to Azure.

**Note:** These instructions describe how to back up the _data_ in your Cassandra ring. You must back up your Cassandra _schemas_ manually in versions prior to 1.0.21.

## S3 Backup

To perform a backup to S3, enter the following command on the DC/OS CLI:

```
$ dcos cassandra --name=<service-name> backup start \
    --backup_name=<backup-name> \
    --external_location=s3://<bucket-name> \
    --s3_access_key=<s3-access-key> \
    --s3_secret_key=<s3-secret-key>
```

To upload to S3, you must specify the "s3://" protocol for the external location along with setting the S3 flags for access key and secret key.

```

To cancel a currently running backup from the CLI, enter the following command:

```
$ dcos cassandra --name=<service-name> backup stop
```

The operation will end after the current node has finished its backup.

## Azure Backup

To perform a backup to Azure, enter the following command on the DC/OS CLI:

```
$ dcos cassandra --name=<service-name> backup start \
    --backup_name=<backup-name> \
    --external_location=azure://<container> \
    --azure_account=<account_name> \
    --azure_key=<key>
```

To upload to Azure, you must specify the "azure://" protocol for the external location along with setting the Azure flags for Azure storage account and a secret key.

To cancel a currently running backup from the CLI, enter the following command:

```
$ dcos cassandra --name=<service-name> backup stop
```

The operation will end after the current node has finished its backup.

# Restore

You can restore your DC/OS Apache Cassandra snapshots on a new Cassandra ring from S3 or from Azure storage.

## S3 Restore

To restore, enter the following command on the DC/OS CLI:

```
$ dcos cassandra --name=<service-name> restore start \
    --backup_name=<backup-name> \
    --external_location=s3://<bucket-name> \
    --s3_access_key=<s3-access-key> \
    --s3_secret_key=<s3-secret-key>
```

To restore from S3, you must specify the "s3://" protocol for the external location along with setting the S3 flags for access key and secret key.

Check the status of the restore:

    $ dcos cassandra --name=<service-name> restore status

## Azure Restore

To restore, enter the following command on the DC/OS CLI:

```
$ dcos cassandra --name=<service-name> restore start \
    --backup_name=<backup-name> \
    --external_location=azure://<container-name> \
    --azure_account=<account_name> \
    --azure_key=<key>
```

To restore from Azure, you must specify the "azure://" protocol for the external location along with setting the Azure flags for Azure storage account and a secret key.

To check the status of the restore from the CLI, enter the following command:

```
$ dcos cassandra --name=<service-name> restore status
```

To cancel a currently running restore from the CLI, enter the following command:

```
$ dcos cassandra --name=<service-name> restore stop
```

The operation will end after the current node has finished its restore.
