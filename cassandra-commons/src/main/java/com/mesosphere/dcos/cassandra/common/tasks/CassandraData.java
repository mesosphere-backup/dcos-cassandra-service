package com.mesosphere.dcos.cassandra.common.tasks;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.mesosphere.dcos.cassandra.common.CassandraProtos;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.tasks.backup.BackupRestoreContext;
import com.mesosphere.dcos.cassandra.common.tasks.cleanup.CleanupContext;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairContext;
import com.mesosphere.dcos.cassandra.common.tasks.upgradesstable.UpgradeSSTableContext;
import org.apache.mesos.Protos;

import java.io.IOException;
import java.util.List;

/**
 * CassandraData encapsulates command task and status data
 */
public class CassandraData {

    public static final CassandraData parse(final ByteString bytes) {
        return new CassandraData(bytes);
    }

    public static final CassandraData createTemplateData() {
        return new CassandraData();
    }

    public static final CassandraData createDaemonData(
        final String hostname,
        final CassandraMode mode,
        final CassandraConfig config) {

        return new CassandraData(
            CassandraTask.TYPE.CASSANDRA_DAEMON,
            hostname,
            mode,
            config);
    }

    public static final CassandraData createDaemonStatusData(
        final CassandraMode mode) {

        return new CassandraData(
            CassandraTask.TYPE.CASSANDRA_DAEMON,
            mode);
    }

    public static final CassandraData createRepairData(
        final String hostname,
        final RepairContext context) {

        return new CassandraData(
            CassandraTask.TYPE.REPAIR,
            hostname,
            context.getNodes(),
            context.getKeySpaces(),
            context.getColumnFamilies());
    }

    public static final CassandraData createRepairStatusData() {
        return new CassandraData(CassandraTask.TYPE.REPAIR);
    }

    public static final CassandraData createCleanupData(
        final String hostname,
        final CleanupContext context) {

        return new CassandraData(
            CassandraTask.TYPE.CLEANUP,
            hostname,
            context.getNodes(),
            context.getKeySpaces(),
            context.getColumnFamilies());
    }

    public static final CassandraData createCleanupStatusData() {
        return new CassandraData(CassandraTask.TYPE.CLEANUP);
    }

    public static final CassandraData createBackupSchemaData(
            final String hostname,
            final BackupRestoreContext context) {

        return new CassandraData(
                CassandraTask.TYPE.BACKUP_SCHEMA,
                hostname,
                context.getNodeId(),
                context.getName(),
                context.getExternalLocation(),
                context.getLocalLocation(),
                context.getAccountId(),
                context.getSecretKey(),
                context.getUsesEmc(),
                context.getRestoreType());
    }

    public static final CassandraData createBackupSchemaStatusData() {
        return new CassandraData(CassandraTask.TYPE.BACKUP_SCHEMA);
    }

    public static final CassandraData createBackupSnapshotData(
            final String hostname,
            final BackupRestoreContext context) {

        return new CassandraData(
                CassandraTask.TYPE.BACKUP_SNAPSHOT,
                hostname,
                context.getNodeId(),
                context.getName(),
                context.getExternalLocation(),
                context.getLocalLocation(),
                context.getAccountId(),
                context.getSecretKey(),
                context.getUsesEmc(),
                context.getRestoreType());
    }

    public static final CassandraData createBackupSnapshotStatusData() {
        return new CassandraData(CassandraTask.TYPE.BACKUP_SNAPSHOT);
    }

    public static final CassandraData createBackupUploadData(
        final String hostname,
        final BackupRestoreContext context) {
        return new CassandraData(
            CassandraTask.TYPE.BACKUP_UPLOAD,
            hostname,
            context.getNodeId(),
            context.getName(),
            context.getExternalLocation(),
            context.getLocalLocation(),
            context.getAccountId(),
            context.getSecretKey(),
            context.getUsesEmc(),
            context.getRestoreType());
    }

    public static final CassandraData createBackupUploadStatusData() {
        return new CassandraData(CassandraTask.TYPE.BACKUP_UPLOAD);
    }

    public static final CassandraData createSnapshotDownloadData(
        final String hostname,
        final BackupRestoreContext context) {
        return new CassandraData(
            CassandraTask.TYPE.SNAPSHOT_DOWNLOAD,
            hostname,
            context.getNodeId(),
            context.getName(),
            context.getExternalLocation(),
            context.getLocalLocation(),
            context.getAccountId(),
            context.getSecretKey(),
            context.getUsesEmc(),
            context.getRestoreType());
    }

    public static final CassandraData createSnapshotDownloadStatusData() {
        return new CassandraData(CassandraTask.TYPE.SNAPSHOT_DOWNLOAD);
    }

    public static final CassandraData createRestoreSnapshotData(
        final String hostname,
        final BackupRestoreContext context) {
        return new CassandraData(
            CassandraTask.TYPE.SNAPSHOT_RESTORE,
            hostname,
            context.getNodeId(),
            context.getName(),
            context.getExternalLocation(),
            context.getLocalLocation(),
            context.getAccountId(),
            context.getSecretKey(),
            context.getUsesEmc(),
            context.getRestoreType());
    }

    public static final CassandraData createRestoreSnapshotStatusData() {
        return new CassandraData(CassandraTask.TYPE.SNAPSHOT_RESTORE);
    }

    public static final CassandraData createRestoreSchemaData(
            final String hostname,
            final BackupRestoreContext context) {
        return new CassandraData(
                CassandraTask.TYPE.SCHEMA_RESTORE,
                hostname,
                context.getNodeId(),
                context.getName(),
                context.getExternalLocation(),
                context.getLocalLocation(),
                context.getAccountId(),
                context.getSecretKey(),
                context.getUsesEmc(),
                context.getRestoreType());
    }

    public static final CassandraData createRestoreSchemaStatusData() {
        return new CassandraData(CassandraTask.TYPE.SCHEMA_RESTORE);
    }

    public static final CassandraData createUpgradeSSTableData(
            final String hostname,
            final UpgradeSSTableContext context) {

        return new CassandraData(
                CassandraTask.TYPE.UPGRADESSTABLE,
                hostname,
                context.getNodes(),
                context.getKeySpaces(),
                context.getColumnFamilies());
    }

    public static final CassandraData createUpgradeSSTableStatusData() {
        return new CassandraData(CassandraTask.TYPE.UPGRADESSTABLE);
    }

    private final CassandraProtos.CassandraData data;

    private CassandraData(final ByteString bytes) {
        try {
            this.data = CassandraProtos.CassandraData.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Invalid ByteString passed to " +
                "CassandraData", e);
        }
    }

    private CassandraData(final CassandraProtos.CassandraData data) {
        this.data = data;
    }

    private CassandraData(final CassandraTask.TYPE type,
                          final String hostname,
                          final CassandraMode mode,
                          final CassandraConfig config) {
        data = CassandraProtos.CassandraData.newBuilder()
            .setType(type.ordinal())
            .setHostname(hostname)
            .setConfig(config.toProto())
            .setMode(mode.ordinal())
            .setState(Protos.TaskState.TASK_STAGING.ordinal())
            .build();
    }

    private CassandraData(final CassandraTask.TYPE type) {
        data = CassandraProtos.CassandraData.newBuilder()
            .setType(type.ordinal())
            .setState(Protos.TaskState.TASK_STAGING.ordinal()).build();
    }

    private CassandraData(final CassandraTask.TYPE type,
                          final CassandraMode mode) {
        data = CassandraProtos.CassandraData.newBuilder()
            .setType(type.ordinal())
            .setMode(mode.ordinal())
            .setState(Protos.TaskState.TASK_STAGING.ordinal()).build();
    }

    private CassandraData(final CassandraTask.TYPE type,
                          final String hostname,
                          final List<String> nodes,
                          final List<String> keySpaces,
                          final List<String> columnFamilies) {

        data = CassandraProtos.CassandraData.newBuilder()
            .setType(type.ordinal())
            .setHostname(hostname)
            .addAllNodes(nodes)
            .addAllKeySpaces(keySpaces)
            .addAllColumnFamilies(columnFamilies)
            .setState(Protos.TaskState.TASK_STAGING.ordinal())
            .build();

    }

    private CassandraData(final CassandraTask.TYPE type,
                          final String hostname,
                          final String nodeId,
                          final String name,
                          final String externalLocation,
                          final String localLocation,
                          final String accountId,
                          final String secretKey,
                          final boolean usesEmc,
                          final String restoreType) {

        data = CassandraProtos.CassandraData.newBuilder()
            .setType(type.ordinal())
            .setHostname(hostname)
            .setNode(nodeId)
            .setBackupName(name)
            .setExternalLocation(externalLocation)
            .setLocalLocation(localLocation)
            .setAccoundId(accountId)
            .setSecretKey(secretKey)
            .setState(Protos.TaskState.TASK_STAGING.ordinal())
            .setUsesEmc(usesEmc)
            .setRestoreType(restoreType)
            .build();

    }

    private CassandraData() {
        data = CassandraProtos.CassandraData.newBuilder()
                .setType(CassandraTask.TYPE.TEMPLATE.ordinal())
                .build();
    }

    private CassandraProtos.CassandraData.Builder getBuilder() {
        return CassandraProtos.CassandraData.newBuilder(data);
    }

    public CassandraTask.TYPE getType() {
        return CassandraTask.TYPE.values()[data.getType()];
    }

    public Protos.TaskState getState() {
        return Protos.TaskState.values()[data.getState()];
    }

    public CassandraMode getMode() {
        return CassandraMode.values()[data.getMode()];
    }

    public CassandraData withState(final Protos.TaskState state) {
        return new CassandraData(
            getBuilder()
                .setState(state.ordinal())
                .build());
    }

    public CassandraData withHostname(final String hostname) {
        return new CassandraData(
            getBuilder()
                .setHostname(hostname)
                .build());
    }

    public CassandraData updateDaemon(final Protos.TaskState state,
                                      final CassandraMode mode,
                                      final CassandraConfig config) {

        return new CassandraData(
            getBuilder().setState(state.ordinal())
                .setMode(mode.ordinal())
                .setConfig(config.toProto()).build());
    }

    public CassandraConfig getConfig() {
        try {
            return CassandraConfig.parse(data.getConfig());
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to parse CassandraConfig " +
                "from Protocol Buffers");
        }
    }

    public CassandraData withNewConfig(CassandraConfig config){
        return new CassandraData(getBuilder()
            .setConfig(config.toProto())
            .build());

    }

    public CassandraData replacing(final String address) {
        return new CassandraData(
            getBuilder().setState(Protos.TaskState.TASK_STAGING
                .ordinal())
                .setMode(CassandraMode.STARTING.ordinal())
                .setConfig(
                    getConfig()
                        .mutable()
                        .setReplaceIp(address)
                        .build()
                        .toProto()).build());
    }

    public String getHostname() {
        return data.getHostname();
    }

    public List<String> getKeySpaces() {
        return data.getKeySpacesList();
    }

    public List<String> getColumnFamilies() {
        return data.getColumnFamiliesList();
    }

    public RepairContext getRepairContext() {
        return new RepairContext(
            data.getNodesList(),
            data.getKeySpacesList(),
            data.getColumnFamiliesList());
    }

    public CleanupContext getCleanupContext() {
        return new CleanupContext(
            data.getNodesList(),
            data.getKeySpacesList(),
            data.getColumnFamiliesList());
    }

    public BackupRestoreContext getBackupRestoreContext() {
        return BackupRestoreContext.create(
            data.getNode(),
            data.getBackupName(),
            data.getExternalLocation(),
            data.getLocalLocation(),
            data.getAccoundId(),
            data.getSecretKey(),
            data.getUsesEmc(),
            data.getRestoreType());
    }

    public UpgradeSSTableContext getUpgradeSSTableContext() {
        return new UpgradeSSTableContext(
                data.getNodesList(),
                data.getKeySpacesList(),
                data.getColumnFamiliesList());
    }

    public ByteString getBytes() {
        return data.toByteString();
    }
}
