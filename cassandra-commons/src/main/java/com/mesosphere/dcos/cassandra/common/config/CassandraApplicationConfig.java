/*
 * Copyright 2016 Mesosphere
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mesosphere.dcos.cassandra.common.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static com.mesosphere.dcos.cassandra.common.util.JsonUtils.MAPPER;
import static com.mesosphere.dcos.cassandra.common.util.JsonUtils.YAML_MAPPER;

/**
 * CassandraApplicationConfig is the configuration class for the
 * Cassandra Daemon process run on Mesos slaves. It is serializable to YAML
 * and JSON and maps to the cassandra.yaml configuration file used by
 * Cassandra to configure the StorageService. The class is immutable, but a
 * mutable Builder can be retrieved to programatically create an instance or
 * modify the contents of an existing instance.
 */
public class CassandraApplicationConfig {

    public static final String DEFAULT_FILE_NAME = "cassandra.yaml";
    public static final String CLUSTER_NAME_KEY = "cluster_name";
    public static final String NUM_TOKENS_KEY = "num_tokens";
    public static final String HINTED_HANDOFF_ENABLED_KEY = "hinted_handoff_enabled";
    public static final String MAX_HINT_WINDOW_IN_MS_KEY = "max_hint_window_in_ms";
    public static final String HINTED_HANDOFF_THROTTLE_IN_KB_KEY = "hinted_handoff_throttle_in_kb";
    public static final String MAX_HINTS_DELIVERY_THREADS_KEY = "max_hints_delivery_threads";
    public static final String BATCHLOG_REPLAY_THROTTLE_IN_KB_KEY = "batchlog_replay_throttle_in_kb";
    public static final String AUTHENTICATOR_KEY = "authenticator";
    public static final String AUTHORIZER_KEY = "authorizer";
    public static final String ROLE_MANAGER_KEY = "role_manager";
    public static final String ROLES_VALIDITY_IN_MS_KEY = "roles_validity_in_ms";
    public static final String PERMISSIONS_VALIDITY_IN_MS_KEY = "permissions_validity_in_ms";
    public static final String PARTITIONER_KEY = "partitioner";
    public static final String PERSISTENT_VOLUME_KEY = "persistent_volume";
    public static final String DATA_FILE_DIRECTORIES_KEY = "data_file_directories";
    public static final String COMMITLOG_DIRECTORY_KEY = "commitlog_directory";
    public static final String DISK_FAILURE_POLICY_KEY = "disk_failure_policy";
    public static final String COMMIT_FAILURE_POLICY_KEY = "commit_failure_policy";
    public static final String KEY_CACHE_SIZE_IN_MB_KEY = "key_cache_size_in_mb";
    public static final String KEY_CACHE_SAVE_PERIOD_KEY = "key_cache_save_period";
    public static final String ROW_CACHE_SIZE_IN_MB_KEY = "row_cache_size_in_mb";
    public static final String ROW_CACHE_SAVE_PERIOD_KEY = "row_cache_save_period";
    public static final String COUNTER_CACHE_SIZE_IN_MB_KEY = "counter_cache_size_in_mb";
    public static final String COUNTER_CACHE_SAVE_PERIOD_KEY = "counter_cache_save_period";
    public static final String SAVED_CACHES_DIRECTORY_KEY = "saved_caches_directory";
    public static final String COMMITLOG_SYNC_KEY = "commitlog_sync";
    public static final String COMMITLOG_SYNC_PERIOD_IN_MS_KEY = "commitlog_sync_period_in_ms";
    public static final String COMMITLOG_SEGMENT_SIZE_IN_MB_KEY = "commitlog_segment_size_in_mb";
    public static final String SEED_PROVIDER_KEY = "seed_provider";
    public static final String CONCURRENT_READS_KEY = "concurrent_reads";
    public static final String CONCURRENT_WRITES_KEY = "concurrent_writes";
    public static final String CONCURRENT_COUNTER_WRITES_KEY = "concurrent_counter_writes";
    public static final String MEMTABLE_ALLOCATION_TYPE_KEY = "memtable_allocation_type";
    public static final String INDEX_SUMMARY_CAPACITY_IN_MB_KEY = "index_summary_capacity_in_mb";
    public static final String INDEX_SUMMARY_RESIZE_INTERVAL_IN_MINUTES_KEY = "index_summary_resize_interval_in_minutes";
    public static final String TRICKLE_FSYNC_KEY = "trickle_fsync";
    public static final String TRICKLE_FSYNC_INTERVAL_IN_KB_KEY = "trickle_fsync_interval_in_kb";
    public static final String STORAGE_PORT_KEY = "storage_port";
    public static final String SSL_STORAGE_PORT_KEY = "ssl_storage_port";
    public static final String LISTEN_ADDRESS_KEY = "listen_address";
    public static final String START_NATIVE_TRANSPORT_KEY = "start_native_transport";
    public static final String NATIVE_TRANSPORT_PORT_KEY = "native_transport_port";
    public static final String START_RPC_KEY = "start_rpc";
    public static final String RPC_ADDRESS_KEY = "rpc_address";
    public static final String RPC_PORT_KEY = "rpc_port";
    public static final String RPC_KEEPALIVE_KEY = "rpc_keepalive";
    public static final String RPC_SERVER_TYPE_KEY = "rpc_server_type";
    public static final String THRIFT_FRAMED_TRANSPORT_SIZE_IN_MB_KEY = "thrift_framed_transport_size_in_mb";
    public static final String INCREMENTAL_BACKUPS_KEY = "incremental_backups";
    public static final String SNAPSHOT_BEFORE_COMPACTION_KEY = "snapshot_before_compaction";
    public static final String AUTO_SNAPSHOT_KEY = "auto_snapshot";
    public static final String TOMBSTONE_WARN_THRESHOLD_KEY = "tombstone_warn_threshold";
    public static final String TOMBSTONE_FAILURE_THRESHOLD_KEY = "tombstone_failure_threshold";
    public static final String COLUMN_INDEX_SIZE_IN_KB_KEY = "column_index_size_in_kb";
    public static final String BATCH_SIZE_WARN_THRESHOLD_IN_KB_KEY = "batch_size_warn_threshold_in_kb";
    public static final String BATCH_SIZE_FAIL_THRESHOLD_IN_KB_KEY = "batch_size_fail_threshold_in_kb";
    public static final String COMPACTION_THROUGHPUT_MB_PER_SEC_KEY = "compaction_throughput_mb_per_sec";
    public static final String COMPACTION_LARGE_PARTITION_WARNING_THRESHOLD_MB_KEY = "compaction_large_partition_warning_threshold_mb";
    public static final String SSTABLE_PREEMPTIVE_OPEN_INTERVAL_IN_MB_KEY = "sstable_preemptive_open_interval_in_mb";
    public static final String READ_REQUEST_TIMEOUT_IN_MS_KEY = "read_request_timeout_in_ms";
    public static final String RANGE_REQUEST_TIMEOUT_IN_MS_KEY = "range_request_timeout_in_ms";
    public static final String WRITE_REQUEST_TIMEOUT_IN_MS_KEY = "write_request_timeout_in_ms";
    public static final String COUNTER_WRITE_REQUEST_TIMEOUT_IN_MS_KEY = "counter_write_request_timeout_in_ms";
    public static final String CAS_CONTENTION_TIMEOUT_IN_MS_KEY = "cas_contention_timeout_in_ms";
    public static final String TRUNCATE_REQUEST_TIMEOUT_IN_MS_KEY = "truncate_request_timeout_in_ms";
    public static final String REQUEST_TIMEOUT_IN_MS_KEY = "request_timeout_in_ms";
    public static final String CROSS_NODE_TIMEOUT_KEY = "cross_node_timeout";
    public static final String ENDPOINT_SNITCH_KEY = "endpoint_snitch";
    public static final String DYNAMIC_SNITCH_UPDATE_INTERVAL_IN_MS_KEY = "dynamic_snitch_update_interval_in_ms";
    public static final String DYNAMIC_SNITCH_RESET_INTERVAL_IN_MS_KEY = "dynamic_snitch_reset_interval_in_ms";
    public static final String DYNAMIC_SNITCH_BADNESS_THRESHOLD_KEY = "dynamic_snitch_badness_threshold";
    public static final String REQUEST_SCHEDULER_KEY = "request_scheduler";
    public static final String SERVER_ENCRYPTION_OPTIONS_KEY = "server_encryption_options";
    public static final String CLIENT_ENCRYPTION_OPTIONS_KEY = "client_encryption_options";
    public static final String INTERNODE_COMPRESSION_KEY = "internode_compression";
    public static final String INTER_DC_TCP_NODELAY_KEY = "inter_dc_tcp_nodelay";
    public static final String TRACETYPE_QUERY_TTL_KEY = "tracetype_query_ttl";
    public static final String TRACETYPE_REPAIR_TTL_KEY = "tracetype_repair_ttl";
    public static final String ENABLE_USER_DEFINED_FUNCTIONS_KEY = "enable_user_defined_functions";
    public static final String WINDOWS_TIMER_INTERVAL_KEY = "windows_timer_interval";

    public static final String DEFAULT_CLUSTER_NAME = "Test Cluster";
    public static final int DEFAULT_NUM_TOKENS = 256;
    public static final boolean DEFAULT_HINTED_HANDOFF_ENABLED = true;
    public static final int DEFAULT_MAX_HINT_WINDOW_IN_MS = 10800000;
    public static final int DEFAULT_HINTED_HANDOFF_THROTTLE_IN_KB = 1024;
    public static final int DEFAULT_MAX_HINTS_DELIVERY_THREADS = 2;
    public static final int DEFAULT_BATCHLOG_REPLAY_THROTTLE_IN_KB = 1024;
    public static final String DEFAULT_AUTHENTICATOR = "AllowAllAuthenticator";
    public static final String DEFAULT_AUTHORIZER = "AllowAllAuthorizer";
    public static final String DEFAULT_ROLE_MANAGER = "CassandraRoleManager";
    public static final int DEFAULT_ROLES_VALIDITY_IN_MS = 2000;
    public static final int DEFAULT_PERMISSIONS_VALIDITY_IN_MS = 2000;
    public static final String DEFAULT_PARTITIONER = "org.apache.cassandra.dht.Murmur3Partitioner";
    public static final String DEFAULT_PERSISTENT_VOLUME = "volume";
    public static final String DEFAULT_DISK_FAILURE_POLICY = "stop";
    public static final String DEFAULT_COMMIT_FAILURE_POLICY = "stop";
    public static final Integer DEFAULT_KEY_CACHE_SIZE_IN_MB = null;
    public static final int DEFAULT_KEY_CACHE_SAVE_PERIOD = 14400;
    public static final int DEFAULT_ROW_CACHE_SIZE_IN_MB = 0;
    public static final int DEFAULT_ROW_CACHE_SAVE_PERIOD = 0;
    public static final Integer DEFAULT_COUNTER_CACHE_SIZE_IN_MB = null;
    public static final int DEFAULT_COUNTER_CACHE_SAVE_PERIOD = 7200;
    public static final String DEFAULT_COMMITLOG_SYNC = "periodic";
    public static final int DEFAULT_COMMITLOG_SYNC_PERIOD_IN_MS = 10000;
    public static final int DEFAULT_COMMITLOG_SEGMENT_SIZE_IN_MB = 32;
    public static final List<Map<String, Object>> DEFAULT_SEED_PROVIDER =
            ImmutableList.<Map<String, Object>>of(
                    ImmutableMap.<String, Object>of(
                            "class_name",
                            "org.apache.cassandra.locator.SimpleSeedProvider",
                            "parameters", ImmutableList.of(ImmutableMap.of
                                    ("seeds", "127.0.0.1"))
                    )
            );
    public static final int DEFAULT_CONCURRENT_READS = 32;
    public static final int DEFAULT_CONCURRENT_WRITES = 32;
    public static final int DEFAULT_CONCURRENT_COUNTER_WRITES = 32;
    public static final String DEFAULT_MEMTABLE_ALLOCATION_TYPE = "heap_buffers";
    public static final Integer DEFAULT_INDEX_SUMMARY_CAPACITY_IN_MB = null;
    public static final int DEFAULT_INDEX_SUMMARY_RESIZE_INTERVAL_IN_MINUTES = 60;
    public static final boolean DEFAULT_TRICKLE_FSYNC = false;
    public static final int DEFAULT_TRICKLE_FSYNC_INTERVAL_IN_KB = 10240;
    public static final int DEFAULT_STORAGE_PORT = 7000;
    public static final int DEFAULT_SSL_STORAGE_PORT = 7001;
    public static final String DEFAULT_LISTEN_ADDRESS = "localhost";
    public static final boolean DEFAULT_START_NATIVE_TRANSPORT = true;
    public static final int DEFAULT_NATIVE_TRANSPORT_PORT = 9042;
    public static final boolean DEFAULT_START_RPC = false;
    public static final String DEFAULT_RPC_ADDRESS = "localhost";
    public static final int DEFAULT_RPC_PORT = 9160;
    public static final boolean DEFAULT_RPC_KEEPALIVE = true;
    public static final String DEFAULT_RPC_SERVER_TYPE = "sync";
    public static final int DEFAULT_THRIFT_FRAMED_TRANSPORT_SIZE_IN_MB = 15;
    public static final boolean DEFAULT_INCREMENTAL_BACKUPS = false;
    public static final boolean DEFAULT_SNAPSHOT_BEFORE_COMPACTION = false;
    public static final boolean DEFAULT_AUTO_SNAPSHOT = true;
    public static final int DEFAULT_TOMBSTONE_WARN_THRESHOLD = 1000;
    public static final int DEFAULT_TOMBSTONE_FAILURE_THRESHOLD = 100000;
    public static final int DEFAULT_COLUMN_INDEX_SIZE_IN_KB = 64;
    public static final int DEFAULT_BATCH_SIZE_WARN_THRESHOLD_IN_KB = 5;
    public static final int DEFAULT_BATCH_SIZE_FAIL_THRESHOLD_IN_KB = 50;
    public static final int DEFAULT_COMPACTION_THROUGHPUT_MB_PER_SEC = 16;
    public static final int DEFAULT_COMPACTION_LARGE_PARTITION_WARNING_THRESHOLD_MB = 100;
    public static final int DEFAULT_SSTABLE_PREEMPTIVE_OPEN_INTERVAL_IN_MB = 50;
    public static final int DEFAULT_READ_REQUEST_TIMEOUT_IN_MS = 5000;
    public static final int DEFAULT_RANGE_REQUEST_TIMEOUT_IN_MS = 10000;
    public static final int DEFAULT_WRITE_REQUEST_TIMEOUT_IN_MS = 2000;
    public static final int DEFAULT_COUNTER_WRITE_REQUEST_TIMEOUT_IN_MS = 5000;
    public static final int DEFAULT_CAS_CONTENTION_TIMEOUT_IN_MS = 1000;
    public static final int DEFAULT_TRUNCATE_REQUEST_TIMEOUT_IN_MS = 60000;
    public static final int DEFAULT_REQUEST_TIMEOUT_IN_MS = 10000;
    public static final boolean DEFAULT_CROSS_NODE_TIMEOUT = false;
    public static final String DEFAULT_ENDPOINT_SNITCH = "GossipingPropertyFileSnitch";
    public static final int DEFAULT_DYNAMIC_SNITCH_UPDATE_INTERVAL_IN_MS = 100;
    public static final int DEFAULT_DYNAMIC_SNITCH_RESET_INTERVAL_IN_MS = 600000;
    public static final double DEFAULT_DYNAMIC_SNITCH_BADNESS_THRESHOLD = 0.1;
    public static final String DEFAULT_REQUEST_SCHEDULER = "org.apache.cassandra.scheduler.NoScheduler";
    public static final String DEFAULT_INTERNODE_COMPRESSION = "all";
    public static final boolean DEFAULT_INTER_DC_TCP_NODELAY = false;
    public static final int DEFAULT_TRACETYPE_QUERY_TTL = 86400;
    public static final int DEFAULT_TRACETYPE_REPAIR_TTL = 604800;
    public static final boolean DEFAULT_ENABLE_USER_DEFINED_FUNCTIONS = false;
    public static final int DEFAULT_WINDOWS_TIMER_INTERVAL = 1;
    public static final Map<String, Object> DEFAULT_SERVER_ENCRYPTION_OPTIONS =
            ImmutableMap.<String, Object>of(
                    "internode_encryption", "none",
                    "keystore", "conf/.keystore",
                    "keystore_password", "cassandra",
                    "truststore", "conf/.truststore",
                    "truststore_password", "cassandra");

    public static final Map<String, Object> DEFAULT_CLIENT_ENCRYPTION_OPTIONS =
            ImmutableMap.<String, Object>of(
                    "enabled", false,
                    "optional", false,
                    "keystore", "conf/.keystore",
                    "keystore_password", "cassandra");

    /**
     * Parses a configuration from bytes.
     *
     * @param bytes A byte array containing a JSON representation of the
     *              configuration.
     * @return A CassandraApplicationConfig parsed from bytes.
     * @throws IOException if a configuration can not be parsed from bytes.
     */
    public static CassandraApplicationConfig parse(byte[] bytes)
            throws IOException {
        return MAPPER.readValue(bytes, CassandraApplicationConfig.class);
    }

    /**
     * Parses a configuration from bytes.
     *
     * @param bytes A ByteString containing a JSON representation of the
     *              configuration.
     * @return A CassandraApplicationConfig parsed from bytes.
     * @throws IOException if a configuration can not be parsed from bytes.
     */
    public static CassandraApplicationConfig parse(ByteString bytes)
            throws IOException {
        return parse(bytes.toByteArray());
    }

    /**
     * Creates a configuration for a SimpleSeedProvider for a Cassandra
     * instance.
     *
     * @param seeds A string representation of the IP addresses of the Cassandra
     *              seed nodes list.
     * @return A Configuration object containing a SimpleSeedProvider
     * configuration that lists seeds as the seeds for the node.
     */
    public static List<Map<String, Object>> createSimpleSeedProvider(
            List<String> seeds) {
        return ImmutableList.<Map<String, Object>>of(
                ImmutableMap.<String, Object>of(
                        "class_name",
                        "org.apache.cassandra.locator.SimpleSeedProvider",
                        "parameters", ImmutableList.of(ImmutableMap.of
                                ("seeds", Joiner.on(",").join(seeds)))
                )
        );
    }

    /**
     * Creates a DcosSeedProvider configuration for Cassandra.
     *
     * @param url The URL of the DCOS Cassandra Service instance that the
     *            Cassandra node will retrive its seeds from.
     * @return The A DcosSeedProvider configuration that will retrieve its seeds
     * from the host indicated by url.
     */
    public static List<Map<String, Object>> createDcosSeedProvider(String url) {
        return ImmutableList.<Map<String, Object>>of(
                ImmutableMap.<String, Object>of(
                        "class_name",
                        "com.mesosphere.dcos.cassandra.DcosSeedProvider",
                        "parameters", ImmutableList.of(ImmutableMap.of
                                ("seeds_url", url))
                )
        );
    }

    @JsonCreator
    public static CassandraApplicationConfig create(
            @JsonProperty(CLUSTER_NAME_KEY) final String clusterName,
            @JsonProperty(NUM_TOKENS_KEY) final int numTokens,
            @JsonProperty(HINTED_HANDOFF_ENABLED_KEY) final boolean hintedHandoffEnabled,
            @JsonProperty(MAX_HINT_WINDOW_IN_MS_KEY) final int maxHintWindowInMs,
            @JsonProperty(HINTED_HANDOFF_THROTTLE_IN_KB_KEY) final int hintedHandoffThrottleInKb,
            @JsonProperty(MAX_HINTS_DELIVERY_THREADS_KEY) final int
                    maxHintsDeliveryThreads,
            @JsonProperty(BATCHLOG_REPLAY_THROTTLE_IN_KB_KEY) final int
                    batchlogReplayThrottleInKb,
            @JsonProperty(AUTHENTICATOR_KEY) final String authenticator,
            @JsonProperty(AUTHORIZER_KEY) final String authorizer,
            @JsonProperty(ROLE_MANAGER_KEY) final String roleManager,
            @JsonProperty(ROLES_VALIDITY_IN_MS_KEY) final int rolesValidityInMs,
            @JsonProperty(PERMISSIONS_VALIDITY_IN_MS_KEY) final int permissionsValidityInMs,
            @JsonProperty(PARTITIONER_KEY) final String partitioner,
            @JsonProperty(PERSISTENT_VOLUME_KEY) final String persistentVolume,
            @JsonProperty(DISK_FAILURE_POLICY_KEY) final String diskFailurePolicy,
            @JsonProperty(COMMIT_FAILURE_POLICY_KEY) final String commitFailurePolicy,
            @JsonProperty(KEY_CACHE_SIZE_IN_MB_KEY) final Integer keyCacheSizeInMb,
            @JsonProperty(KEY_CACHE_SAVE_PERIOD_KEY) final int keyCacheSavePeriod,
            @JsonProperty(ROW_CACHE_SIZE_IN_MB_KEY) final int rowCacheSizeInMb,
            @JsonProperty(ROW_CACHE_SAVE_PERIOD_KEY) final int rowCacheSavePeriod,
            @JsonProperty(COUNTER_CACHE_SIZE_IN_MB_KEY) final Integer counterCacheSizeInMb,
            @JsonProperty(COUNTER_CACHE_SAVE_PERIOD_KEY) final int counterCacheSavePeriod,
            @JsonProperty(COMMITLOG_SYNC_KEY) final String commitlogSync,
            @JsonProperty(COMMITLOG_SYNC_PERIOD_IN_MS_KEY) final int commitlogSyncPeriodInMs,
            @JsonProperty(COMMITLOG_SEGMENT_SIZE_IN_MB_KEY) final int commitlogSegmentSizeInMb,
            @JsonProperty(SEED_PROVIDER_KEY) final List<Map<String, Object>> seedProvider,
            @JsonProperty(CONCURRENT_READS_KEY) final int concurrentReads,
            @JsonProperty(CONCURRENT_WRITES_KEY) final int concurrentWrites,
            @JsonProperty(CONCURRENT_COUNTER_WRITES_KEY) final int concurrentCounterWrites,
            @JsonProperty(MEMTABLE_ALLOCATION_TYPE_KEY) final String memtableAllocationType,
            @JsonProperty(INDEX_SUMMARY_CAPACITY_IN_MB_KEY) final Integer indexSummaryCapacityInMb,
            @JsonProperty(INDEX_SUMMARY_RESIZE_INTERVAL_IN_MINUTES_KEY) final int indexSummaryResizeIntervalInMinutes,
            @JsonProperty(TRICKLE_FSYNC_KEY) final boolean trickleFsync,
            @JsonProperty(TRICKLE_FSYNC_INTERVAL_IN_KB_KEY) final int trickleFsyncIntervalInKb,
            @JsonProperty(STORAGE_PORT_KEY) final int storagePort,
            @JsonProperty(SSL_STORAGE_PORT_KEY) final int sslStoragePort,
            @JsonProperty(LISTEN_ADDRESS_KEY) final String listenAddress,
            @JsonProperty(START_NATIVE_TRANSPORT_KEY) final boolean startNativeTransport,
            @JsonProperty(NATIVE_TRANSPORT_PORT_KEY) final int nativeTransportPort,
            @JsonProperty(START_RPC_KEY) final boolean startRpc,
            @JsonProperty(RPC_ADDRESS_KEY) final String rpcAddress,
            @JsonProperty(RPC_PORT_KEY) final int rpcPort,
            @JsonProperty(RPC_KEEPALIVE_KEY) final boolean rpcKeepalive,
            @JsonProperty(RPC_SERVER_TYPE_KEY) final String rpcServerType,
            @JsonProperty(THRIFT_FRAMED_TRANSPORT_SIZE_IN_MB_KEY) final int thriftFramedTransportSizeInMb,
            @JsonProperty(INCREMENTAL_BACKUPS_KEY) final boolean incrementalBackups,
            @JsonProperty(SNAPSHOT_BEFORE_COMPACTION_KEY) final boolean snapshotBeforeCompaction,
            @JsonProperty(AUTO_SNAPSHOT_KEY) final boolean autoSnapshot,
            @JsonProperty(TOMBSTONE_WARN_THRESHOLD_KEY) final int tombstoneWarnThreshold,
            @JsonProperty(TOMBSTONE_FAILURE_THRESHOLD_KEY) final int tombstoneFailureThreshold,
            @JsonProperty(COLUMN_INDEX_SIZE_IN_KB_KEY) final int columnIndexSizeInKb,
            @JsonProperty(BATCH_SIZE_WARN_THRESHOLD_IN_KB_KEY) final int batchSizeWarnThresholdInKb,
            @JsonProperty(BATCH_SIZE_FAIL_THRESHOLD_IN_KB_KEY) final int batchSizeFailThresholdInKb,
            @JsonProperty(COMPACTION_THROUGHPUT_MB_PER_SEC_KEY) final int compactionThroughputMbPerSec,
            @JsonProperty
                    (COMPACTION_LARGE_PARTITION_WARNING_THRESHOLD_MB_KEY) final int compactionLargePartitionWarningThresholdMb,
            @JsonProperty(SSTABLE_PREEMPTIVE_OPEN_INTERVAL_IN_MB_KEY) final int sstablePreemptiveOpenIntervalInMb,
            @JsonProperty(READ_REQUEST_TIMEOUT_IN_MS_KEY) final int readRequestTimeoutInMs,
            @JsonProperty(RANGE_REQUEST_TIMEOUT_IN_MS_KEY) final int rangeRequestTimeoutInMs,
            @JsonProperty(WRITE_REQUEST_TIMEOUT_IN_MS_KEY) final int writeRequestTimeoutInMs,
            @JsonProperty(COUNTER_WRITE_REQUEST_TIMEOUT_IN_MS_KEY) final int counterWriteRequestTimeoutInMs,
            @JsonProperty(CAS_CONTENTION_TIMEOUT_IN_MS_KEY) final int casContentionTimeoutInMs,
            @JsonProperty(TRUNCATE_REQUEST_TIMEOUT_IN_MS_KEY) final int truncateRequestTimeoutInMs,
            @JsonProperty(REQUEST_TIMEOUT_IN_MS_KEY) final int requestTimeoutInMs,
            @JsonProperty(CROSS_NODE_TIMEOUT_KEY) final boolean crossNodeTimeout,
            @JsonProperty(ENDPOINT_SNITCH_KEY) final String endpointSnitch,
            @JsonProperty(DYNAMIC_SNITCH_UPDATE_INTERVAL_IN_MS_KEY) final int dynamicSnitchUpdateIntervalInMs,
            @JsonProperty(DYNAMIC_SNITCH_RESET_INTERVAL_IN_MS_KEY) final int dynamicSnitchResetIntervalInMs,
            @JsonProperty(DYNAMIC_SNITCH_BADNESS_THRESHOLD_KEY) final double dynamicSnitchBadnessThreshold,
            @JsonProperty(REQUEST_SCHEDULER_KEY) final String requestScheduler,
            @JsonProperty(INTERNODE_COMPRESSION_KEY) final String internodeCompression,
            @JsonProperty(INTER_DC_TCP_NODELAY_KEY) final boolean interDcTcpNodelay,
            @JsonProperty(TRACETYPE_QUERY_TTL_KEY) final int tracetypeQueryTtl,
            @JsonProperty(TRACETYPE_REPAIR_TTL_KEY) final int tracetypeRepairTtl,
            @JsonProperty(ENABLE_USER_DEFINED_FUNCTIONS_KEY) final boolean enableUserDefinedFunctions,
            @JsonProperty(WINDOWS_TIMER_INTERVAL_KEY) final int windowsTimerInterval) {

        return new CassandraApplicationConfig(clusterName,
                numTokens,
                hintedHandoffEnabled,
                maxHintWindowInMs,
                hintedHandoffThrottleInKb,
                maxHintsDeliveryThreads,
                batchlogReplayThrottleInKb,
                authenticator,
                authorizer,
                roleManager,
                rolesValidityInMs,
                permissionsValidityInMs,
                partitioner,
                persistentVolume,
                diskFailurePolicy,
                commitFailurePolicy,
                keyCacheSizeInMb,
                keyCacheSavePeriod,
                rowCacheSizeInMb,
                rowCacheSavePeriod,
                counterCacheSizeInMb,
                counterCacheSavePeriod,
                commitlogSync,
                commitlogSyncPeriodInMs,
                commitlogSegmentSizeInMb,
                seedProvider,
                concurrentReads,
                concurrentWrites,
                concurrentCounterWrites,
                memtableAllocationType,
                indexSummaryCapacityInMb,
                indexSummaryResizeIntervalInMinutes,
                trickleFsync,
                trickleFsyncIntervalInKb,
                storagePort,
                sslStoragePort,
                listenAddress,
                startNativeTransport,
                nativeTransportPort,
                startRpc,
                rpcAddress,
                rpcPort,
                rpcKeepalive,
                rpcServerType,
                thriftFramedTransportSizeInMb,
                incrementalBackups,
                snapshotBeforeCompaction,
                autoSnapshot,
                tombstoneWarnThreshold,
                tombstoneFailureThreshold,
                columnIndexSizeInKb,
                batchSizeWarnThresholdInKb,
                batchSizeFailThresholdInKb,
                compactionThroughputMbPerSec,
                compactionLargePartitionWarningThresholdMb,
                sstablePreemptiveOpenIntervalInMb,
                readRequestTimeoutInMs,
                rangeRequestTimeoutInMs,
                writeRequestTimeoutInMs,
                counterWriteRequestTimeoutInMs,
                casContentionTimeoutInMs,
                truncateRequestTimeoutInMs,
                requestTimeoutInMs,
                crossNodeTimeout,
                endpointSnitch,
                dynamicSnitchUpdateIntervalInMs,
                dynamicSnitchResetIntervalInMs,
                dynamicSnitchBadnessThreshold,
                requestScheduler,
                internodeCompression,
                interDcTcpNodelay,
                tracetypeQueryTtl,
                tracetypeRepairTtl,
                enableUserDefinedFunctions,
                windowsTimerInterval);

    }

    public static Builder builder() {
        return new Builder();
    }

    @JsonProperty(CLUSTER_NAME_KEY)
    private final String clusterName;
    @JsonProperty(NUM_TOKENS_KEY)
    private final int numTokens;
    @JsonProperty(HINTED_HANDOFF_ENABLED_KEY)
    private final boolean hintedHandoffEnabled;
    @JsonProperty(MAX_HINT_WINDOW_IN_MS_KEY)
    private final int maxHintWindowInMs;
    @JsonProperty(HINTED_HANDOFF_THROTTLE_IN_KB_KEY)
    private final int hintedHandoffThrottleInKb;
    @JsonProperty(MAX_HINTS_DELIVERY_THREADS_KEY)
    private final int maxHintsDeliveryThreads;
    @JsonProperty(BATCHLOG_REPLAY_THROTTLE_IN_KB_KEY)
    private final int batchlogReplayThrottleInKb;
    @JsonProperty(AUTHENTICATOR_KEY)
    private final String authenticator;
    @JsonProperty(AUTHORIZER_KEY)
    private final String authorizer;
    @JsonProperty(ROLE_MANAGER_KEY)
    private final String roleManager;
    @JsonProperty(ROLES_VALIDITY_IN_MS_KEY)
    private final int rolesValidityInMs;
    @JsonProperty(PERMISSIONS_VALIDITY_IN_MS_KEY)
    private final int permissionsValidityInMs;
    @JsonProperty(PARTITIONER_KEY)
    private final String partitioner;
    @JsonProperty(PERSISTENT_VOLUME_KEY)
    private final String persistentVolume;
    @JsonProperty(DISK_FAILURE_POLICY_KEY)
    private final String diskFailurePolicy;
    @JsonProperty(COMMIT_FAILURE_POLICY_KEY)
    private final String commitFailurePolicy;
    @JsonProperty(KEY_CACHE_SIZE_IN_MB_KEY)
    private final Integer keyCacheSizeInMb;
    @JsonProperty(KEY_CACHE_SAVE_PERIOD_KEY)
    private final int keyCacheSavePeriod;
    @JsonProperty(ROW_CACHE_SIZE_IN_MB_KEY)
    private final int rowCacheSizeInMb;
    @JsonProperty(ROW_CACHE_SAVE_PERIOD_KEY)
    private final int rowCacheSavePeriod;
    @JsonProperty(COUNTER_CACHE_SIZE_IN_MB_KEY)
    private final Integer counterCacheSizeInMb;
    @JsonProperty(COUNTER_CACHE_SAVE_PERIOD_KEY)
    private final int counterCacheSavePeriod;
    @JsonProperty(COMMITLOG_SYNC_KEY)
    private final String commitlogSync;
    @JsonProperty(COMMITLOG_SYNC_PERIOD_IN_MS_KEY)
    private final int commitlogSyncPeriodInMs;
    @JsonProperty(COMMITLOG_SEGMENT_SIZE_IN_MB_KEY)
    private final int commitlogSegmentSizeInMb;
    @JsonProperty(SEED_PROVIDER_KEY)
    private final List<Map<String, Object>> seedProvider;
    @JsonProperty(CONCURRENT_READS_KEY)
    private final int concurrentReads;
    @JsonProperty(CONCURRENT_WRITES_KEY)
    private final int concurrentWrites;
    @JsonProperty(CONCURRENT_COUNTER_WRITES_KEY)
    private final int concurrentCounterWrites;
    @JsonProperty(MEMTABLE_ALLOCATION_TYPE_KEY)
    private final String memtableAllocationType;
    @JsonProperty(INDEX_SUMMARY_CAPACITY_IN_MB_KEY)
    private final Integer indexSummaryCapacityInMb;
    @JsonProperty(INDEX_SUMMARY_RESIZE_INTERVAL_IN_MINUTES_KEY)
    private final int indexSummaryResizeIntervalInMinutes;
    @JsonProperty(TRICKLE_FSYNC_KEY)
    private final boolean trickleFsync;
    @JsonProperty(TRICKLE_FSYNC_INTERVAL_IN_KB_KEY)
    private final int trickleFsyncIntervalInKb;
    @JsonProperty(STORAGE_PORT_KEY)
    private final int storagePort;
    @JsonProperty(SSL_STORAGE_PORT_KEY)
    private final int sslStoragePort;
    @JsonProperty(LISTEN_ADDRESS_KEY)
    private final String listenAddress;
    @JsonProperty(START_NATIVE_TRANSPORT_KEY)
    private final boolean startNativeTransport;
    @JsonProperty(NATIVE_TRANSPORT_PORT_KEY)
    private final int nativeTransportPort;
    @JsonProperty(START_RPC_KEY)
    private final boolean startRpc;
    @JsonProperty("rpcAddress")
    private final String rpcAddress;
    @JsonProperty(RPC_PORT_KEY)
    private final int rpcPort;
    @JsonProperty(RPC_KEEPALIVE_KEY)
    private final boolean rpcKeepalive;
    @JsonProperty(RPC_SERVER_TYPE_KEY)
    private final String rpcServerType;
    @JsonProperty(THRIFT_FRAMED_TRANSPORT_SIZE_IN_MB_KEY)
    private final int thriftFramedTransportSizeInMb;
    @JsonProperty(INCREMENTAL_BACKUPS_KEY)
    private final boolean incrementalBackups;
    @JsonProperty(SNAPSHOT_BEFORE_COMPACTION_KEY)
    private final boolean snapshotBeforeCompaction;
    @JsonProperty(AUTO_SNAPSHOT_KEY)
    private final boolean autoSnapshot;
    @JsonProperty(TOMBSTONE_WARN_THRESHOLD_KEY)
    private final int tombstoneWarnThreshold;
    @JsonProperty(TOMBSTONE_FAILURE_THRESHOLD_KEY)
    private final int tombstoneFailureThreshold;
    @JsonProperty(COLUMN_INDEX_SIZE_IN_KB_KEY)
    private final int columnIndexSizeInKb;
    @JsonProperty(BATCH_SIZE_WARN_THRESHOLD_IN_KB_KEY)
    private final int batchSizeWarnThresholdInKb;
    @JsonProperty(BATCH_SIZE_FAIL_THRESHOLD_IN_KB_KEY)
    private final int batchSizeFailThresholdInKb;
    @JsonProperty(COMPACTION_THROUGHPUT_MB_PER_SEC_KEY)
    private final int compactionThroughputMbPerSec;
    @JsonProperty(COMPACTION_LARGE_PARTITION_WARNING_THRESHOLD_MB_KEY)
    private final int compactionLargePartitionWarningThresholdMb;
    @JsonProperty(SSTABLE_PREEMPTIVE_OPEN_INTERVAL_IN_MB_KEY)
    private final int sstablePreemptiveOpenIntervalInMb;
    @JsonProperty(READ_REQUEST_TIMEOUT_IN_MS_KEY)
    private final int readRequestTimeoutInMs;
    @JsonProperty(RANGE_REQUEST_TIMEOUT_IN_MS_KEY)
    private final int rangeRequestTimeoutInMs;
    @JsonProperty(WRITE_REQUEST_TIMEOUT_IN_MS_KEY)
    private final int writeRequestTimeoutInMs;
    @JsonProperty(COUNTER_WRITE_REQUEST_TIMEOUT_IN_MS_KEY)
    private final int counterWriteRequestTimeoutInMs;
    @JsonProperty(CAS_CONTENTION_TIMEOUT_IN_MS_KEY)
    private final int casContentionTimeoutInMs;
    @JsonProperty(TRUNCATE_REQUEST_TIMEOUT_IN_MS_KEY)
    private final int truncateRequestTimeoutInMs;
    @JsonProperty(REQUEST_TIMEOUT_IN_MS_KEY)
    private final int requestTimeoutInMs;
    @JsonProperty(CROSS_NODE_TIMEOUT_KEY)
    private final boolean crossNodeTimeout;
    @JsonProperty(ENDPOINT_SNITCH_KEY)
    private final String endpointSnitch;
    @JsonProperty(DYNAMIC_SNITCH_UPDATE_INTERVAL_IN_MS_KEY)
    private final int dynamicSnitchUpdateIntervalInMs;
    @JsonProperty(DYNAMIC_SNITCH_RESET_INTERVAL_IN_MS_KEY)
    private final int dynamicSnitchResetIntervalInMs;
    @JsonProperty(DYNAMIC_SNITCH_BADNESS_THRESHOLD_KEY)
    private final double dynamicSnitchBadnessThreshold;
    @JsonProperty(REQUEST_SCHEDULER_KEY)
    private final String requestScheduler;
    @JsonProperty(INTERNODE_COMPRESSION_KEY)
    private final String internodeCompression;
    @JsonProperty(INTER_DC_TCP_NODELAY_KEY)
    private final boolean interDcTcpNodelay;
    @JsonProperty(TRACETYPE_QUERY_TTL_KEY)
    private final int tracetypeQueryTtl;
    @JsonProperty(TRACETYPE_REPAIR_TTL_KEY)
    private final int tracetypeRepairTtl;
    @JsonProperty(ENABLE_USER_DEFINED_FUNCTIONS_KEY)
    private final boolean enableUserDefinedFunctions;
    @JsonProperty(WINDOWS_TIMER_INTERVAL_KEY)
    private final int windowsTimerInterval;

    public CassandraApplicationConfig(
            String clusterName,
            int numTokens,
            boolean hintedHandoffEnabled,
            int maxHintWindowInMs,
            int hintedHandoffThrottleInKb,
            int maxHintsDeliveryThreads,
            int batchlogReplayThrottleInKb,
            String authenticator,
            String authorizer,
            String roleManager,
            int rolesValidityInMs,
            int permissionsValidityInMs,
            String partitioner,
            String persistentVolume,
            String diskFailurePolicy,
            String commitFailurePolicy,
            Integer keyCacheSizeInMb,
            int keyCacheSavePeriod,
            int rowCacheSizeInMb,
            int rowCacheSavePeriod,
            Integer counterCacheSizeInMb,
            int counterCacheSavePeriod,
            String commitlogSync,
            int commitlogSyncPeriodInMs,
            int commitlogSegmentSizeInMb,
            List<Map<String, Object>> seedProvider,
            int concurrentReads,
            int concurrentWrites,
            int concurrentCounterWrites,
            String memtableAllocationType,
            Integer indexSummaryCapacityInMb,
            int indexSummaryResizeIntervalInMinutes,
            boolean trickleFsync,
            int trickleFsyncIntervalInKb,
            int storagePort,
            int sslStoragePort,
            String listenAddress,
            boolean startNativeTransport,
            int nativeTransportPort,
            boolean startRpc,
            String rpcAddress,
            int rpcPort,
            boolean rpcKeepalive,
            String rpcServerType,
            int thriftFramedTransportSizeInMb,
            boolean incrementalBackups,
            boolean snapshotBeforeCompaction,
            boolean autoSnapshot,
            int tombstoneWarnThreshold,
            int tombstoneFailureThreshold,
            int columnIndexSizeInKb,
            int batchSizeWarnThresholdInKb,
            int batchSizeFailThresholdInKb,
            int compactionThroughputMbPerSec,
            int compactionLargePartitionWarningThresholdMb,
            int sstablePreemptiveOpenIntervalInMb,
            int readRequestTimeoutInMs,
            int rangeRequestTimeoutInMs,
            int writeRequestTimeoutInMs,
            int counterWriteRequestTimeoutInMs,
            int casContentionTimeoutInMs,
            int truncateRequestTimeoutInMs,
            int requestTimeoutInMs,
            boolean crossNodeTimeout,
            String endpointSnitch,
            int dynamicSnitchUpdateIntervalInMs,
            int dynamicSnitchResetIntervalInMs,
            double dynamicSnitchBadnessThreshold,
            String requestScheduler,
            String internodeCompression,
            boolean interDcTcpNodelay,
            int tracetypeQueryTtl,
            int tracetypeRepairTtl,
            boolean enableUserDefinedFunctions,
            int windowsTimerInterval) {
        this.clusterName = clusterName;
        this.numTokens = numTokens;
        this.hintedHandoffEnabled = hintedHandoffEnabled;
        this.maxHintWindowInMs = maxHintWindowInMs;
        this.hintedHandoffThrottleInKb = hintedHandoffThrottleInKb;
        this.maxHintsDeliveryThreads = maxHintsDeliveryThreads;
        this.batchlogReplayThrottleInKb = batchlogReplayThrottleInKb;
        this.authenticator = authenticator;
        this.authorizer = authorizer;
        this.roleManager = roleManager;
        this.rolesValidityInMs = rolesValidityInMs;
        this.permissionsValidityInMs = permissionsValidityInMs;
        this.partitioner = partitioner;
        this.persistentVolume = persistentVolume;
        this.diskFailurePolicy = diskFailurePolicy;
        this.commitFailurePolicy = commitFailurePolicy;
        this.keyCacheSizeInMb = keyCacheSizeInMb;
        this.keyCacheSavePeriod = keyCacheSavePeriod;
        this.rowCacheSizeInMb = rowCacheSizeInMb;
        this.rowCacheSavePeriod = rowCacheSavePeriod;
        this.counterCacheSizeInMb = counterCacheSizeInMb;
        this.counterCacheSavePeriod = counterCacheSavePeriod;
        this.commitlogSync = commitlogSync;
        this.commitlogSyncPeriodInMs = commitlogSyncPeriodInMs;
        this.commitlogSegmentSizeInMb = commitlogSegmentSizeInMb;
        this.seedProvider = seedProvider;
        this.concurrentReads = concurrentReads;
        this.concurrentWrites = concurrentWrites;
        this.concurrentCounterWrites = concurrentCounterWrites;
        this.memtableAllocationType = memtableAllocationType;
        this.indexSummaryCapacityInMb = indexSummaryCapacityInMb;
        this.indexSummaryResizeIntervalInMinutes = indexSummaryResizeIntervalInMinutes;
        this.trickleFsync = trickleFsync;
        this.trickleFsyncIntervalInKb = trickleFsyncIntervalInKb;
        this.storagePort = storagePort;
        this.sslStoragePort = sslStoragePort;
        this.listenAddress = listenAddress;
        this.startNativeTransport = startNativeTransport;
        this.nativeTransportPort = nativeTransportPort;
        this.startRpc = startRpc;
        this.rpcAddress = rpcAddress;
        this.rpcPort = rpcPort;
        this.rpcKeepalive = rpcKeepalive;
        this.rpcServerType = rpcServerType;
        this.thriftFramedTransportSizeInMb = thriftFramedTransportSizeInMb;
        this.incrementalBackups = incrementalBackups;
        this.snapshotBeforeCompaction = snapshotBeforeCompaction;
        this.autoSnapshot = autoSnapshot;
        this.tombstoneWarnThreshold = tombstoneWarnThreshold;
        this.tombstoneFailureThreshold = tombstoneFailureThreshold;
        this.columnIndexSizeInKb = columnIndexSizeInKb;
        this.batchSizeWarnThresholdInKb = batchSizeWarnThresholdInKb;
        this.batchSizeFailThresholdInKb = batchSizeFailThresholdInKb;
        this.compactionThroughputMbPerSec = compactionThroughputMbPerSec;
        this.compactionLargePartitionWarningThresholdMb = compactionLargePartitionWarningThresholdMb;
        this.sstablePreemptiveOpenIntervalInMb = sstablePreemptiveOpenIntervalInMb;
        this.readRequestTimeoutInMs = readRequestTimeoutInMs;
        this.rangeRequestTimeoutInMs = rangeRequestTimeoutInMs;
        this.writeRequestTimeoutInMs = writeRequestTimeoutInMs;
        this.counterWriteRequestTimeoutInMs = counterWriteRequestTimeoutInMs;
        this.casContentionTimeoutInMs = casContentionTimeoutInMs;
        this.truncateRequestTimeoutInMs = truncateRequestTimeoutInMs;
        this.requestTimeoutInMs = requestTimeoutInMs;
        this.crossNodeTimeout = crossNodeTimeout;
        this.endpointSnitch = endpointSnitch;
        this.dynamicSnitchUpdateIntervalInMs = dynamicSnitchUpdateIntervalInMs;
        this.dynamicSnitchResetIntervalInMs = dynamicSnitchResetIntervalInMs;
        this.dynamicSnitchBadnessThreshold = dynamicSnitchBadnessThreshold;
        this.requestScheduler = requestScheduler;
        this.internodeCompression = internodeCompression;
        this.interDcTcpNodelay = interDcTcpNodelay;
        this.tracetypeQueryTtl = tracetypeQueryTtl;
        this.tracetypeRepairTtl = tracetypeRepairTtl;
        this.enableUserDefinedFunctions = enableUserDefinedFunctions;
        this.windowsTimerInterval = windowsTimerInterval;
    }

    public String getClusterName() {
        return clusterName;
    }

    public int getWindowsTimerInterval() {
        return windowsTimerInterval;
    }

    public int getDynamicSnitchUpdateIntervalInMs() {
        return dynamicSnitchUpdateIntervalInMs;
    }

    public int getDynamicSnitchResetIntervalInMs() {
        return dynamicSnitchResetIntervalInMs;
    }

    public double getDynamicSnitchBadnessThreshold() {
        return dynamicSnitchBadnessThreshold;
    }

    public String getRequestScheduler() {
        return requestScheduler;
    }

    public String getInternodeCompression() {
        return internodeCompression;
    }

    public boolean isInterDcTcpNodelay() {
        return interDcTcpNodelay;
    }

    public int getTracetypeQueryTtl() {
        return tracetypeQueryTtl;
    }

    public int getTracetypeRepairTtl() {
        return tracetypeRepairTtl;
    }

    public boolean isEnableUserDefinedFunctions() {
        return enableUserDefinedFunctions;
    }

    public int getNumTokens() {
        return numTokens;
    }

    public boolean isHintedHandoffEnabled() {
        return hintedHandoffEnabled;
    }

    public int getMaxHintWindowInMs() {
        return maxHintWindowInMs;
    }

    public int getHintedHandoffThrottleInKb() {
        return hintedHandoffThrottleInKb;
    }

    public int getMaxHintsDeliveryThreads() {
        return maxHintsDeliveryThreads;
    }

    public int getBatchlogReplayThrottleInKb() {
        return batchlogReplayThrottleInKb;
    }

    public String getAuthenticator() {
        return authenticator;
    }

    public String getAuthorizer() {
        return authorizer;
    }

    public String getRoleManager() {
        return roleManager;
    }

    public int getRolesValidityInMs() {
        return rolesValidityInMs;
    }

    public int getPermissionsValidityInMs() {
        return permissionsValidityInMs;
    }

    public String getPartitioner() {
        return partitioner;
    }

    public String getPersistentVolume() {
        return persistentVolume;
    }

    public String getDiskFailurePolicy() {
        return diskFailurePolicy;
    }

    public String getCommitFailurePolicy() {
        return commitFailurePolicy;
    }

    public Integer getKeyCacheSizeInMb() {
        return keyCacheSizeInMb;
    }

    public int getKeyCacheSavePeriod() {
        return keyCacheSavePeriod;
    }

    public int getRowCacheSizeInMb() {
        return rowCacheSizeInMb;
    }

    public int getRowCacheSavePeriod() {
        return rowCacheSavePeriod;
    }

    public Integer getCounterCacheSizeInMb() {
        return counterCacheSizeInMb;
    }

    public int getCounterCacheSavePeriod() {
        return counterCacheSavePeriod;
    }

    public String getCommitlogSync() {
        return commitlogSync;
    }

    public int getCommitlogSyncPeriodInMs() {
        return commitlogSyncPeriodInMs;
    }

    public int getCommitlogSegmentSizeInMb() {
        return commitlogSegmentSizeInMb;
    }

    public int getConcurrentReads() {
        return concurrentReads;
    }

    public int getConcurrentWrites() {
        return concurrentWrites;
    }

    public int getConcurrentCounterWrites() {
        return concurrentCounterWrites;
    }

    public String getMemtableAllocationType() {
        return memtableAllocationType;
    }

    public Integer getIndexSummaryCapacityInMb() {
        return indexSummaryCapacityInMb;
    }

    public int getIndexSummaryResizeIntervalInMinutes() {
        return indexSummaryResizeIntervalInMinutes;
    }

    public boolean isTrickleFsync() {
        return trickleFsync;
    }

    public int getTrickleFsyncIntervalInKb() {
        return trickleFsyncIntervalInKb;
    }

    public int getStoragePort() {
        return storagePort;
    }

    public int getSslStoragePort() {
        return sslStoragePort;
    }


    public String getListenAddress() {
        return listenAddress;
    }

    public boolean isStartNativeTransport() {
        return startNativeTransport;
    }

    public int getNativeTransportPort() {
        return nativeTransportPort;
    }

    public boolean isStartRpc() {
        return startRpc;
    }

    public String getRpcAddress() {
        return rpcAddress;
    }

    public int getRpcPort() {
        return rpcPort;
    }

    public boolean isRpcKeepalive() {
        return rpcKeepalive;
    }

    public String getRpcServerType() {
        return rpcServerType;
    }

    public int getThriftFramedTransportSizeInMb() {
        return thriftFramedTransportSizeInMb;
    }

    public boolean isIncrementalBackups() {
        return incrementalBackups;
    }

    public boolean isSnapshotBeforeCompaction() {
        return snapshotBeforeCompaction;
    }

    public boolean isAutoSnapshot() {
        return autoSnapshot;
    }

    public int getTombstoneWarnThreshold() {
        return tombstoneWarnThreshold;
    }

    public int getTombstoneFailureThreshold() {
        return tombstoneFailureThreshold;
    }

    public int getColumnIndexSizeInKb() {
        return columnIndexSizeInKb;
    }

    public int getBatchSizeWarnThresholdInKb() {
        return batchSizeWarnThresholdInKb;
    }

    public int getBatchSizeFailThresholdInKb() {
        return batchSizeFailThresholdInKb;
    }

    public int getCompactionThroughputMbPerSec() {
        return compactionThroughputMbPerSec;
    }

    public int getCompactionLargePartitionWarningThresholdMb() {
        return compactionLargePartitionWarningThresholdMb;
    }

    public int getSstablePreemptiveOpenIntervalInMb() {
        return sstablePreemptiveOpenIntervalInMb;
    }

    public int getReadRequestTimeoutInMs() {
        return readRequestTimeoutInMs;
    }

    public int getRangeRequestTimeoutInMs() {
        return rangeRequestTimeoutInMs;
    }

    public int getWriteRequestTimeoutInMs() {
        return writeRequestTimeoutInMs;
    }

    public int getCounterWriteRequestTimeoutInMs() {
        return counterWriteRequestTimeoutInMs;
    }

    public int getCasContentionTimeoutInMs() {
        return casContentionTimeoutInMs;
    }

    public int getTruncateRequestTimeoutInMs() {
        return truncateRequestTimeoutInMs;
    }

    public Builder toBuilder() {
        return new Builder(this);
    }

    public int getRequestTimeoutInMs() {
        return requestTimeoutInMs;
    }

    public boolean isCrossNodeTimeout() {
        return crossNodeTimeout;
    }

    public String getEndpointSnitch() {
        return endpointSnitch;
    }

    public Map<String, Object> toMap() {

        Map<String, Object> map = new HashMap<>(100);

        map.put(CLUSTER_NAME_KEY, clusterName);
        map.put(NUM_TOKENS_KEY, numTokens);
        map.put(HINTED_HANDOFF_ENABLED_KEY, hintedHandoffEnabled);
        map.put(MAX_HINT_WINDOW_IN_MS_KEY, maxHintWindowInMs);
        map.put(HINTED_HANDOFF_THROTTLE_IN_KB_KEY, hintedHandoffThrottleInKb);
        map.put(MAX_HINTS_DELIVERY_THREADS_KEY, maxHintsDeliveryThreads);
        map.put(BATCHLOG_REPLAY_THROTTLE_IN_KB_KEY, batchlogReplayThrottleInKb);
        map.put(AUTHENTICATOR_KEY, authenticator);
        map.put(AUTHORIZER_KEY, authorizer);
        map.put(ROLE_MANAGER_KEY, roleManager);
        map.put(ROLES_VALIDITY_IN_MS_KEY, rolesValidityInMs);
        map.put(PERMISSIONS_VALIDITY_IN_MS_KEY, permissionsValidityInMs);
        map.put(PARTITIONER_KEY, partitioner);
        map.put(DATA_FILE_DIRECTORIES_KEY,
                Arrays.asList(Paths.get(persistentVolume, "data").toString()));
        map.put(COMMITLOG_DIRECTORY_KEY,
                Paths.get(persistentVolume, "commitlog").toString());
        map.put(SAVED_CACHES_DIRECTORY_KEY, Paths.get(persistentVolume,
                "saved_caches").toString());
        map.put(DISK_FAILURE_POLICY_KEY, diskFailurePolicy);
        map.put(COMMIT_FAILURE_POLICY_KEY, commitFailurePolicy);
        map.put(KEY_CACHE_SIZE_IN_MB_KEY, keyCacheSizeInMb);
        map.put(KEY_CACHE_SAVE_PERIOD_KEY, keyCacheSavePeriod);
        map.put(ROW_CACHE_SIZE_IN_MB_KEY, rowCacheSizeInMb);
        map.put(ROW_CACHE_SAVE_PERIOD_KEY, rowCacheSavePeriod);
        map.put(COUNTER_CACHE_SIZE_IN_MB_KEY, counterCacheSizeInMb);
        map.put(COUNTER_CACHE_SAVE_PERIOD_KEY, counterCacheSavePeriod);
        map.put(COMMITLOG_SYNC_KEY, commitlogSync);
        map.put(COMMITLOG_SYNC_PERIOD_IN_MS_KEY, commitlogSyncPeriodInMs);
        map.put(COMMITLOG_SEGMENT_SIZE_IN_MB_KEY, commitlogSegmentSizeInMb);
        map.put(SEED_PROVIDER_KEY, seedProvider);
        map.put(CONCURRENT_READS_KEY, concurrentReads);
        map.put(CONCURRENT_WRITES_KEY, concurrentWrites);
        map.put(CONCURRENT_COUNTER_WRITES_KEY, concurrentCounterWrites);
        map.put(MEMTABLE_ALLOCATION_TYPE_KEY, memtableAllocationType);
        map.put(INDEX_SUMMARY_CAPACITY_IN_MB_KEY, indexSummaryCapacityInMb);
        map.put(INDEX_SUMMARY_RESIZE_INTERVAL_IN_MINUTES_KEY,
                indexSummaryResizeIntervalInMinutes);
        map.put(TRICKLE_FSYNC_KEY, trickleFsync);
        map.put(TRICKLE_FSYNC_INTERVAL_IN_KB_KEY, trickleFsyncIntervalInKb);
        map.put(STORAGE_PORT_KEY, storagePort);
        map.put(SSL_STORAGE_PORT_KEY, sslStoragePort);
        map.put(LISTEN_ADDRESS_KEY, listenAddress);
        map.put(START_NATIVE_TRANSPORT_KEY, startNativeTransport);
        map.put(NATIVE_TRANSPORT_PORT_KEY, nativeTransportPort);
        map.put(START_RPC_KEY, startRpc);
        map.put(RPC_ADDRESS_KEY, rpcAddress);
        map.put(RPC_PORT_KEY, rpcPort);
        map.put(RPC_KEEPALIVE_KEY, rpcKeepalive);
        map.put(RPC_SERVER_TYPE_KEY, rpcServerType);
        map.put(THRIFT_FRAMED_TRANSPORT_SIZE_IN_MB_KEY,
                thriftFramedTransportSizeInMb);
        map.put(INCREMENTAL_BACKUPS_KEY, incrementalBackups);
        map.put(SNAPSHOT_BEFORE_COMPACTION_KEY, snapshotBeforeCompaction);
        map.put(AUTO_SNAPSHOT_KEY, autoSnapshot);
        map.put(TOMBSTONE_WARN_THRESHOLD_KEY, tombstoneWarnThreshold);
        map.put(TOMBSTONE_FAILURE_THRESHOLD_KEY, tombstoneFailureThreshold);
        map.put(COLUMN_INDEX_SIZE_IN_KB_KEY, columnIndexSizeInKb);
        map.put(BATCH_SIZE_WARN_THRESHOLD_IN_KB_KEY,
                batchSizeWarnThresholdInKb);
        map.put(BATCH_SIZE_FAIL_THRESHOLD_IN_KB_KEY,
                batchSizeFailThresholdInKb);
        map.put(COMPACTION_THROUGHPUT_MB_PER_SEC_KEY,
                compactionThroughputMbPerSec);
        map.put(COMPACTION_LARGE_PARTITION_WARNING_THRESHOLD_MB_KEY,
                compactionLargePartitionWarningThresholdMb);
        map.put(SSTABLE_PREEMPTIVE_OPEN_INTERVAL_IN_MB_KEY,
                sstablePreemptiveOpenIntervalInMb);
        map.put(READ_REQUEST_TIMEOUT_IN_MS_KEY, readRequestTimeoutInMs);
        map.put(RANGE_REQUEST_TIMEOUT_IN_MS_KEY, rangeRequestTimeoutInMs);
        map.put(WRITE_REQUEST_TIMEOUT_IN_MS_KEY, writeRequestTimeoutInMs);
        map.put(COUNTER_WRITE_REQUEST_TIMEOUT_IN_MS_KEY,
                counterWriteRequestTimeoutInMs);
        map.put(CAS_CONTENTION_TIMEOUT_IN_MS_KEY, casContentionTimeoutInMs);
        map.put(TRUNCATE_REQUEST_TIMEOUT_IN_MS_KEY, truncateRequestTimeoutInMs);
        map.put(REQUEST_TIMEOUT_IN_MS_KEY, requestTimeoutInMs);
        map.put(CROSS_NODE_TIMEOUT_KEY, crossNodeTimeout);
        map.put(ENDPOINT_SNITCH_KEY, endpointSnitch);
        map.put(DYNAMIC_SNITCH_UPDATE_INTERVAL_IN_MS_KEY,
                dynamicSnitchUpdateIntervalInMs);
        map.put(DYNAMIC_SNITCH_RESET_INTERVAL_IN_MS_KEY,
                dynamicSnitchResetIntervalInMs);
        map.put(DYNAMIC_SNITCH_BADNESS_THRESHOLD_KEY,
                dynamicSnitchBadnessThreshold);
        map.put(REQUEST_SCHEDULER_KEY, requestScheduler);
        map.put(INTERNODE_COMPRESSION_KEY, internodeCompression);
        map.put(INTER_DC_TCP_NODELAY_KEY, interDcTcpNodelay);
        map.put(TRACETYPE_QUERY_TTL_KEY, tracetypeQueryTtl);
        map.put(TRACETYPE_REPAIR_TTL_KEY, tracetypeRepairTtl);
        map.put(ENABLE_USER_DEFINED_FUNCTIONS_KEY, enableUserDefinedFunctions);
        map.put(WINDOWS_TIMER_INTERVAL_KEY, windowsTimerInterval);
        map.put(CLIENT_ENCRYPTION_OPTIONS_KEY,
                DEFAULT_CLIENT_ENCRYPTION_OPTIONS);
        map.put(SERVER_ENCRYPTION_OPTIONS_KEY,
                DEFAULT_SERVER_ENCRYPTION_OPTIONS);

        return map;
    }

    public void writeDaemonConfiguration(final Path path) throws IOException {

        YAML_MAPPER.writeValue(path.toFile(), toMap());
    }

    public byte[] toByteArray() throws JsonProcessingException {

        return MAPPER.writeValueAsBytes(this);

    }

    public ByteString toByteString() throws JsonProcessingException {

        return ByteString.copyFrom(toByteArray());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CassandraApplicationConfig)) return false;
        CassandraApplicationConfig that = (CassandraApplicationConfig) o;
        return isHintedHandoffEnabled() == that.isHintedHandoffEnabled() &&
                getMaxHintWindowInMs() == that.getMaxHintWindowInMs() &&
                getHintedHandoffThrottleInKb() == that.getHintedHandoffThrottleInKb() &&
                getMaxHintsDeliveryThreads() == that.getMaxHintsDeliveryThreads() &&
                getBatchlogReplayThrottleInKb() == that.getBatchlogReplayThrottleInKb() &&
                getRolesValidityInMs() == that.getRolesValidityInMs() &&
                getPermissionsValidityInMs() == that.getPermissionsValidityInMs() &&
                getKeyCacheSavePeriod() == that.getKeyCacheSavePeriod() &&
                getRowCacheSizeInMb() == that.getRowCacheSizeInMb() &&
                getRowCacheSavePeriod() == that.getRowCacheSavePeriod() &&
                getCounterCacheSavePeriod() == that.getCounterCacheSavePeriod() &&
                getCommitlogSyncPeriodInMs() == that.getCommitlogSyncPeriodInMs() &&
                getCommitlogSegmentSizeInMb() == that.getCommitlogSegmentSizeInMb() &&
                getConcurrentReads() == that.getConcurrentReads() &&
                getConcurrentWrites() == that.getConcurrentWrites() &&
                getConcurrentCounterWrites() == that.getConcurrentCounterWrites() &&
                getIndexSummaryResizeIntervalInMinutes() == that.getIndexSummaryResizeIntervalInMinutes() &&
                isTrickleFsync() == that.isTrickleFsync() &&
                getTrickleFsyncIntervalInKb() == that.getTrickleFsyncIntervalInKb() &&
                getStoragePort() == that.getStoragePort() &&
                getSslStoragePort() == that.getSslStoragePort() &&
                isStartNativeTransport() == that.isStartNativeTransport() &&
                getNativeTransportPort() == that.getNativeTransportPort() &&
                isStartRpc() == that.isStartRpc() &&
                getRpcPort() == that.getRpcPort() &&
                isRpcKeepalive() == that.isRpcKeepalive() &&
                getThriftFramedTransportSizeInMb() == that.getThriftFramedTransportSizeInMb() &&
                isIncrementalBackups() == that.isIncrementalBackups() &&
                isSnapshotBeforeCompaction() == that.isSnapshotBeforeCompaction() &&
                isAutoSnapshot() == that.isAutoSnapshot() &&
                getTombstoneWarnThreshold() == that.getTombstoneWarnThreshold() &&
                getTombstoneFailureThreshold() == that.getTombstoneFailureThreshold() &&
                getColumnIndexSizeInKb() == that.getColumnIndexSizeInKb() &&
                getBatchSizeWarnThresholdInKb() == that.getBatchSizeWarnThresholdInKb() &&
                getBatchSizeFailThresholdInKb() == that.getBatchSizeFailThresholdInKb() &&
                getCompactionThroughputMbPerSec() == that.getCompactionThroughputMbPerSec() &&
                getCompactionLargePartitionWarningThresholdMb() == that.getCompactionLargePartitionWarningThresholdMb() &&
                getSstablePreemptiveOpenIntervalInMb() == that.getSstablePreemptiveOpenIntervalInMb() &&
                getReadRequestTimeoutInMs() == that.getReadRequestTimeoutInMs() &&
                getRangeRequestTimeoutInMs() == that.getRangeRequestTimeoutInMs() &&
                getWriteRequestTimeoutInMs() == that.getWriteRequestTimeoutInMs() &&
                getCounterWriteRequestTimeoutInMs() == that.getCounterWriteRequestTimeoutInMs() &&
                getCasContentionTimeoutInMs() == that.getCasContentionTimeoutInMs() &&
                getTruncateRequestTimeoutInMs() == that.getTruncateRequestTimeoutInMs() &&
                getRequestTimeoutInMs() == that.getRequestTimeoutInMs() &&
                isCrossNodeTimeout() == that.isCrossNodeTimeout() &&
                getDynamicSnitchUpdateIntervalInMs() == that.getDynamicSnitchUpdateIntervalInMs() &&
                getDynamicSnitchResetIntervalInMs() == that.getDynamicSnitchResetIntervalInMs() &&
                Double.compare(that.getDynamicSnitchBadnessThreshold(),
                        getDynamicSnitchBadnessThreshold()) == 0 &&
                isInterDcTcpNodelay() == that.isInterDcTcpNodelay() &&
                getTracetypeQueryTtl() == that.getTracetypeQueryTtl() &&
                getTracetypeRepairTtl() == that.getTracetypeRepairTtl() &&
                isEnableUserDefinedFunctions() == that.isEnableUserDefinedFunctions() &&
                getWindowsTimerInterval() == that.getWindowsTimerInterval() &&
                Objects.equals(getClusterName(), that.getClusterName()) &&
                Objects.equals(getAuthenticator(),
                        that.getAuthenticator()) &&
                Objects.equals(getAuthorizer(), that.getAuthorizer()) &&
                Objects.equals(getRoleManager(), that.getRoleManager()) &&
                Objects.equals(getPartitioner(), that.getPartitioner()) &&
                Objects.equals(getDiskFailurePolicy(),
                        that.getDiskFailurePolicy()) &&
                Objects.equals(getCommitFailurePolicy(),
                        that.getCommitFailurePolicy()) &&
                Objects.equals(getKeyCacheSizeInMb(),
                        that.getKeyCacheSizeInMb()) &&
                Objects.equals(getCounterCacheSizeInMb(),
                        that.getCounterCacheSizeInMb()) &&
                Objects.equals(getCommitlogSync(),
                        that.getCommitlogSync()) &&
                Objects.equals(getMemtableAllocationType(),
                        that.getMemtableAllocationType()) &&
                Objects.equals(getIndexSummaryCapacityInMb(),
                        that.getIndexSummaryCapacityInMb()) &&
                Objects.equals(getRpcServerType(),
                        that.getRpcServerType()) &&
                Objects.equals(getEndpointSnitch(),
                        that.getEndpointSnitch()) &&
                Objects.equals(getRequestScheduler(),
                        that.getRequestScheduler()) &&
                Objects.equals(getInternodeCompression(),
                        that.getInternodeCompression());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClusterName(), isHintedHandoffEnabled(),
                getMaxHintWindowInMs(), getHintedHandoffThrottleInKb(),
                getMaxHintsDeliveryThreads(), getBatchlogReplayThrottleInKb(),
                getAuthenticator(), getAuthorizer(), getRoleManager(),
                getRolesValidityInMs(), getPermissionsValidityInMs(),
                getPartitioner(), getDiskFailurePolicy(),
                getCommitFailurePolicy(),
                getKeyCacheSizeInMb(), getKeyCacheSavePeriod(),
                getRowCacheSizeInMb(), getRowCacheSavePeriod(),
                getCounterCacheSizeInMb(), getCounterCacheSavePeriod(),
                getCommitlogSync(), getCommitlogSyncPeriodInMs(),
                getCommitlogSegmentSizeInMb(), getConcurrentReads(),
                getConcurrentWrites(), getConcurrentCounterWrites(),
                getMemtableAllocationType(), getIndexSummaryCapacityInMb(),
                getIndexSummaryResizeIntervalInMinutes(), isTrickleFsync(),
                getTrickleFsyncIntervalInKb(), getStoragePort(),
                getSslStoragePort(), isStartNativeTransport(),
                getNativeTransportPort(), isStartRpc(), getRpcPort(),
                isRpcKeepalive(), getRpcServerType(),
                getThriftFramedTransportSizeInMb(), isIncrementalBackups(),
                isSnapshotBeforeCompaction(), isAutoSnapshot(),
                getTombstoneWarnThreshold(), getTombstoneFailureThreshold(),
                getColumnIndexSizeInKb(), getBatchSizeWarnThresholdInKb(),
                getBatchSizeFailThresholdInKb(),
                getCompactionThroughputMbPerSec(),
                getCompactionLargePartitionWarningThresholdMb(),
                getSstablePreemptiveOpenIntervalInMb(),
                getReadRequestTimeoutInMs(),
                getRangeRequestTimeoutInMs(), getWriteRequestTimeoutInMs(),
                getCounterWriteRequestTimeoutInMs(),
                getCasContentionTimeoutInMs(),
                getTruncateRequestTimeoutInMs(), getRequestTimeoutInMs(),
                isCrossNodeTimeout(), getEndpointSnitch(),
                getDynamicSnitchUpdateIntervalInMs(),
                getDynamicSnitchResetIntervalInMs(),
                getDynamicSnitchBadnessThreshold(), getRequestScheduler(),
                getInternodeCompression(), isInterDcTcpNodelay(),
                getTracetypeQueryTtl(), getTracetypeRepairTtl(),
                isEnableUserDefinedFunctions(), getWindowsTimerInterval());
    }

    @Override
    public String toString() {
        return JsonUtils.toJsonString(this);
    }

    public static final class Builder {

        private String clusterName;
        private int numTokens;
        private boolean hintedHandoffEnabled;
        private int maxHintWindowInMs;
        private int hintedHandoffThrottleInKb;
        private int maxHintsDeliveryThreads;
        private int batchlogReplayThrottleInKb;
        private String authenticator;
        private String authorizer;
        private String roleManager;
        private int rolesValidityInMs;
        private int permissionsValidityInMs;
        private String partitioner;
        private String persistentVolume;
        private String diskFailurePolicy;
        private String commitFailurePolicy;
        private Integer keyCacheSizeInMb;
        private int keyCacheSavePeriod;
        private int rowCacheSizeInMb;
        private int rowCacheSavePeriod;
        private Integer counterCacheSizeInMb;
        private int counterCacheSavePeriod;
        private String commitlogSync;
        private int commitlogSyncPeriodInMs;
        private int commitlogSegmentSizeInMb;
        private List<Map<String, Object>> seedProvider;
        private int concurrentReads;
        private int concurrentWrites;
        private int concurrentCounterWrites;
        private String memtableAllocationType;
        private Integer indexSummaryCapacityInMb;
        private int indexSummaryResizeIntervalInMinutes;
        private boolean trickleFsync;
        private int trickleFsyncIntervalInKb;
        private int storagePort;
        private int sslStoragePort;
        private String listenAddress;
        private boolean startNativeTransport;
        private int nativeTransportPort;
        private boolean startRpc;
        private String rpcAddress;
        private int rpcPort;
        private boolean rpcKeepalive;
        private String rpcServerType;
        private int thriftFramedTransportSizeInMb;
        private boolean incrementalBackups;
        private boolean snapshotBeforeCompaction;
        private boolean autoSnapshot;
        private int tombstoneWarnThreshold;
        private int tombstoneFailureThreshold;
        private int columnIndexSizeInKb;
        private int batchSizeWarnThresholdInKb;
        private int batchSizeFailThresholdInKb;
        private int compactionThroughputMbPerSec;
        private int compactionLargePartitionWarningThresholdMb;
        private int sstablePreemptiveOpenIntervalInMb;
        private int readRequestTimeoutInMs;
        private int rangeRequestTimeoutInMs;
        private int writeRequestTimeoutInMs;
        private int counterWriteRequestTimeoutInMs;
        private int casContentionTimeoutInMs;
        private int truncateRequestTimeoutInMs;
        private int requestTimeoutInMs;
        private boolean crossNodeTimeout;
        private String endpointSnitch;
        private int dynamicSnitchUpdateIntervalInMs;
        private int dynamicSnitchResetIntervalInMs;
        private double dynamicSnitchBadnessThreshold;
        private String requestScheduler;
        private String internodeCompression;
        private boolean interDcTcpNodelay;
        private int tracetypeQueryTtl;
        private int tracetypeRepairTtl;
        private boolean enableUserDefinedFunctions;
        private int windowsTimerInterval;

        private Builder() {

            clusterName = DEFAULT_CLUSTER_NAME;
            numTokens = DEFAULT_NUM_TOKENS;
            hintedHandoffEnabled = DEFAULT_HINTED_HANDOFF_ENABLED;
            maxHintWindowInMs = DEFAULT_MAX_HINT_WINDOW_IN_MS;
            hintedHandoffThrottleInKb = DEFAULT_HINTED_HANDOFF_THROTTLE_IN_KB;
            maxHintsDeliveryThreads = DEFAULT_MAX_HINTS_DELIVERY_THREADS;
            batchlogReplayThrottleInKb = DEFAULT_BATCHLOG_REPLAY_THROTTLE_IN_KB;
            authenticator = DEFAULT_AUTHENTICATOR;
            authorizer = DEFAULT_AUTHORIZER;
            roleManager = DEFAULT_ROLE_MANAGER;
            rolesValidityInMs = DEFAULT_ROLES_VALIDITY_IN_MS;
            permissionsValidityInMs = DEFAULT_PERMISSIONS_VALIDITY_IN_MS;
            partitioner = DEFAULT_PARTITIONER;
            persistentVolume = DEFAULT_PERSISTENT_VOLUME;
            diskFailurePolicy = DEFAULT_DISK_FAILURE_POLICY;
            commitFailurePolicy = DEFAULT_COMMIT_FAILURE_POLICY;
            keyCacheSizeInMb = DEFAULT_KEY_CACHE_SIZE_IN_MB;
            keyCacheSavePeriod = DEFAULT_KEY_CACHE_SAVE_PERIOD;
            rowCacheSizeInMb = DEFAULT_ROW_CACHE_SIZE_IN_MB;
            rowCacheSavePeriod = DEFAULT_ROW_CACHE_SAVE_PERIOD;
            counterCacheSizeInMb = DEFAULT_COUNTER_CACHE_SIZE_IN_MB;
            counterCacheSavePeriod = DEFAULT_COUNTER_CACHE_SAVE_PERIOD;
            commitlogSync = DEFAULT_COMMITLOG_SYNC;
            commitlogSyncPeriodInMs = DEFAULT_COMMITLOG_SYNC_PERIOD_IN_MS;
            commitlogSegmentSizeInMb = DEFAULT_COMMITLOG_SEGMENT_SIZE_IN_MB;
            seedProvider = DEFAULT_SEED_PROVIDER;
            concurrentReads = DEFAULT_CONCURRENT_READS;
            concurrentWrites = DEFAULT_CONCURRENT_WRITES;
            concurrentCounterWrites = DEFAULT_CONCURRENT_COUNTER_WRITES;
            memtableAllocationType = DEFAULT_MEMTABLE_ALLOCATION_TYPE;
            indexSummaryCapacityInMb = DEFAULT_INDEX_SUMMARY_CAPACITY_IN_MB;
            indexSummaryResizeIntervalInMinutes = DEFAULT_INDEX_SUMMARY_RESIZE_INTERVAL_IN_MINUTES;
            trickleFsync = DEFAULT_TRICKLE_FSYNC;
            trickleFsyncIntervalInKb = DEFAULT_TRICKLE_FSYNC_INTERVAL_IN_KB;
            storagePort = DEFAULT_STORAGE_PORT;
            sslStoragePort = DEFAULT_SSL_STORAGE_PORT;
            listenAddress = DEFAULT_LISTEN_ADDRESS;
            startNativeTransport = DEFAULT_START_NATIVE_TRANSPORT;
            nativeTransportPort = DEFAULT_NATIVE_TRANSPORT_PORT;
            startRpc = DEFAULT_START_RPC;
            rpcAddress = DEFAULT_RPC_ADDRESS;
            rpcPort = DEFAULT_RPC_PORT;
            rpcKeepalive = DEFAULT_RPC_KEEPALIVE;
            rpcServerType = DEFAULT_RPC_SERVER_TYPE;
            thriftFramedTransportSizeInMb = DEFAULT_THRIFT_FRAMED_TRANSPORT_SIZE_IN_MB;
            incrementalBackups = DEFAULT_INCREMENTAL_BACKUPS;
            snapshotBeforeCompaction = DEFAULT_SNAPSHOT_BEFORE_COMPACTION;
            autoSnapshot = DEFAULT_AUTO_SNAPSHOT;
            tombstoneWarnThreshold = DEFAULT_TOMBSTONE_WARN_THRESHOLD;
            tombstoneFailureThreshold = DEFAULT_TOMBSTONE_FAILURE_THRESHOLD;
            columnIndexSizeInKb = DEFAULT_COLUMN_INDEX_SIZE_IN_KB;
            batchSizeWarnThresholdInKb = DEFAULT_BATCH_SIZE_WARN_THRESHOLD_IN_KB;
            batchSizeFailThresholdInKb = DEFAULT_BATCH_SIZE_FAIL_THRESHOLD_IN_KB;
            compactionThroughputMbPerSec = DEFAULT_COMPACTION_THROUGHPUT_MB_PER_SEC;
            compactionLargePartitionWarningThresholdMb = DEFAULT_COMPACTION_LARGE_PARTITION_WARNING_THRESHOLD_MB;
            sstablePreemptiveOpenIntervalInMb = DEFAULT_SSTABLE_PREEMPTIVE_OPEN_INTERVAL_IN_MB;
            readRequestTimeoutInMs = DEFAULT_READ_REQUEST_TIMEOUT_IN_MS;
            rangeRequestTimeoutInMs = DEFAULT_RANGE_REQUEST_TIMEOUT_IN_MS;
            writeRequestTimeoutInMs = DEFAULT_WRITE_REQUEST_TIMEOUT_IN_MS;
            counterWriteRequestTimeoutInMs = DEFAULT_COUNTER_WRITE_REQUEST_TIMEOUT_IN_MS;
            casContentionTimeoutInMs = DEFAULT_CAS_CONTENTION_TIMEOUT_IN_MS;
            truncateRequestTimeoutInMs = DEFAULT_TRUNCATE_REQUEST_TIMEOUT_IN_MS;
            requestTimeoutInMs = DEFAULT_REQUEST_TIMEOUT_IN_MS;
            crossNodeTimeout = DEFAULT_CROSS_NODE_TIMEOUT;
            endpointSnitch = DEFAULT_ENDPOINT_SNITCH;
            dynamicSnitchUpdateIntervalInMs = DEFAULT_DYNAMIC_SNITCH_UPDATE_INTERVAL_IN_MS;
            dynamicSnitchResetIntervalInMs = DEFAULT_DYNAMIC_SNITCH_RESET_INTERVAL_IN_MS;
            dynamicSnitchBadnessThreshold = DEFAULT_DYNAMIC_SNITCH_BADNESS_THRESHOLD;
            requestScheduler = DEFAULT_REQUEST_SCHEDULER;
            internodeCompression = DEFAULT_INTERNODE_COMPRESSION;
            interDcTcpNodelay = DEFAULT_INTER_DC_TCP_NODELAY;
            tracetypeQueryTtl = DEFAULT_TRACETYPE_QUERY_TTL;
            tracetypeRepairTtl = DEFAULT_TRACETYPE_REPAIR_TTL;
            enableUserDefinedFunctions = DEFAULT_ENABLE_USER_DEFINED_FUNCTIONS;
            windowsTimerInterval = DEFAULT_WINDOWS_TIMER_INTERVAL;
        }

        private Builder(CassandraApplicationConfig config) {

            this.clusterName = config.clusterName;
            this.numTokens = config.numTokens;
            this.hintedHandoffEnabled = config.hintedHandoffEnabled;
            this.maxHintWindowInMs = config.maxHintWindowInMs;
            this.hintedHandoffThrottleInKb = config.hintedHandoffThrottleInKb;
            this.maxHintsDeliveryThreads = config.maxHintsDeliveryThreads;
            this.batchlogReplayThrottleInKb = config.batchlogReplayThrottleInKb;
            this.authenticator = config.authenticator;
            this.authorizer = config.authorizer;
            this.roleManager = config.roleManager;
            this.rolesValidityInMs = config.rolesValidityInMs;
            this.permissionsValidityInMs = config.permissionsValidityInMs;
            this.partitioner = config.partitioner;
            this.persistentVolume = config.persistentVolume;
            this.diskFailurePolicy = config.diskFailurePolicy;
            this.commitFailurePolicy = config.commitFailurePolicy;
            this.keyCacheSizeInMb = config.keyCacheSizeInMb;
            this.keyCacheSavePeriod = config.keyCacheSavePeriod;
            this.rowCacheSizeInMb = config.rowCacheSizeInMb;
            this.rowCacheSavePeriod = config.rowCacheSavePeriod;
            this.counterCacheSizeInMb = config.counterCacheSizeInMb;
            this.counterCacheSavePeriod = config.counterCacheSavePeriod;
            this.commitlogSync = config.commitlogSync;
            this.commitlogSyncPeriodInMs = config.commitlogSyncPeriodInMs;
            this.commitlogSegmentSizeInMb = config.commitlogSegmentSizeInMb;
            this.seedProvider = config.seedProvider;
            this.concurrentReads = config.concurrentReads;
            this.concurrentWrites = config.concurrentWrites;
            this.concurrentCounterWrites = config.concurrentCounterWrites;
            this.memtableAllocationType = config.memtableAllocationType;
            this.indexSummaryCapacityInMb = config.indexSummaryCapacityInMb;
            this.indexSummaryResizeIntervalInMinutes = config.indexSummaryResizeIntervalInMinutes;
            this.trickleFsync = config.trickleFsync;
            this.trickleFsyncIntervalInKb = config.trickleFsyncIntervalInKb;
            this.storagePort = config.storagePort;
            this.sslStoragePort = config.sslStoragePort;
            this.listenAddress = config.listenAddress;
            this.startNativeTransport = config.startNativeTransport;
            this.nativeTransportPort = config.nativeTransportPort;
            this.startRpc = config.startRpc;
            this.rpcAddress = config.rpcAddress;
            this.rpcPort = config.rpcPort;
            this.rpcKeepalive = config.rpcKeepalive;
            this.rpcServerType = config.rpcServerType;
            this.thriftFramedTransportSizeInMb = config.thriftFramedTransportSizeInMb;
            this.incrementalBackups = config.incrementalBackups;
            this.snapshotBeforeCompaction = config.snapshotBeforeCompaction;
            this.autoSnapshot = config.autoSnapshot;
            this.tombstoneWarnThreshold = config.tombstoneWarnThreshold;
            this.tombstoneFailureThreshold = config.tombstoneFailureThreshold;
            this.columnIndexSizeInKb = config.columnIndexSizeInKb;
            this.batchSizeWarnThresholdInKb = config.batchSizeWarnThresholdInKb;
            this.batchSizeFailThresholdInKb = config.batchSizeFailThresholdInKb;
            this.compactionThroughputMbPerSec = config.compactionThroughputMbPerSec;
            this.compactionLargePartitionWarningThresholdMb = config.compactionLargePartitionWarningThresholdMb;
            this.sstablePreemptiveOpenIntervalInMb = config.sstablePreemptiveOpenIntervalInMb;
            this.readRequestTimeoutInMs = config.readRequestTimeoutInMs;
            this.rangeRequestTimeoutInMs = config.rangeRequestTimeoutInMs;
            this.writeRequestTimeoutInMs = config.writeRequestTimeoutInMs;
            this.counterWriteRequestTimeoutInMs = config.counterWriteRequestTimeoutInMs;
            this.casContentionTimeoutInMs = config.casContentionTimeoutInMs;
            this.truncateRequestTimeoutInMs = config.truncateRequestTimeoutInMs;
            this.requestTimeoutInMs = config.requestTimeoutInMs;
            this.crossNodeTimeout = config.crossNodeTimeout;
            this.endpointSnitch = config.endpointSnitch;
            this.dynamicSnitchUpdateIntervalInMs = config.dynamicSnitchUpdateIntervalInMs;
            this.dynamicSnitchResetIntervalInMs = config.dynamicSnitchResetIntervalInMs;
            this.dynamicSnitchBadnessThreshold = config.dynamicSnitchBadnessThreshold;
            this.requestScheduler = config.requestScheduler;
            this.internodeCompression = config.internodeCompression;
            this.interDcTcpNodelay = config.interDcTcpNodelay;
            this.tracetypeQueryTtl = config.tracetypeQueryTtl;
            this.tracetypeRepairTtl = config.tracetypeRepairTtl;
            this.enableUserDefinedFunctions = config.enableUserDefinedFunctions;
            this.windowsTimerInterval = config.windowsTimerInterval;
        }

        public String getClusterName() {
            return clusterName;
        }

        public int getNumTokens() {
            return numTokens;
        }

        public boolean isHintedHandoffEnabled() {
            return hintedHandoffEnabled;
        }

        public int getMaxHintWindowInMs() {
            return maxHintWindowInMs;
        }

        public int getHintedHandoffThrottleInKb() {
            return hintedHandoffThrottleInKb;
        }

        public int getMaxHintsDeliveryThreads() {
            return maxHintsDeliveryThreads;
        }

        public int getBatchlogReplayThrottleInKb() {
            return batchlogReplayThrottleInKb;
        }

        public String getAuthenticator() {
            return authenticator;
        }

        public String getAuthorizer() {
            return authorizer;
        }

        public String getRoleManager() {
            return roleManager;
        }

        public int getRolesValidityInMs() {
            return rolesValidityInMs;
        }

        public int getPermissionsValidityInMs() {
            return permissionsValidityInMs;
        }

        public String getPartitioner() {
            return partitioner;
        }

        public String getPersistentVolume() {
            return persistentVolume;
        }

        public String getDiskFailurePolicy() {
            return diskFailurePolicy;
        }

        public String getCommitFailurePolicy() {
            return commitFailurePolicy;
        }

        public Integer getKeyCacheSizeInMb() {
            return keyCacheSizeInMb;
        }

        public int getKeyCacheSavePeriod() {
            return keyCacheSavePeriod;
        }

        public int getRowCacheSizeInMb() {
            return rowCacheSizeInMb;
        }

        public int getRowCacheSavePeriod() {
            return rowCacheSavePeriod;
        }

        public Integer getCounterCacheSizeInMb() {
            return counterCacheSizeInMb;
        }

        public int getCounterCacheSavePeriod() {
            return counterCacheSavePeriod;
        }

        public String getCommitlogSync() {
            return commitlogSync;
        }

        public int getCommitlogSyncPeriodInMs() {
            return commitlogSyncPeriodInMs;
        }

        public int getCommitlogSegmentSizeInMb() {
            return commitlogSegmentSizeInMb;
        }

        public List<Map<String, Object>> getSeedProvider() {
            return seedProvider;
        }

        public int getConcurrentReads() {
            return concurrentReads;
        }

        public int getConcurrentWrites() {
            return concurrentWrites;
        }

        public int getConcurrentCounterWrites() {
            return concurrentCounterWrites;
        }

        public String getMemtableAllocationType() {
            return memtableAllocationType;
        }

        public Integer getIndexSummaryCapacityInMb() {
            return indexSummaryCapacityInMb;
        }

        public int getIndexSummaryResizeIntervalInMinutes() {
            return indexSummaryResizeIntervalInMinutes;
        }

        public boolean isTrickleFsync() {
            return trickleFsync;
        }

        public int getTrickleFsyncIntervalInKb() {
            return trickleFsyncIntervalInKb;
        }

        public int getStoragePort() {
            return storagePort;
        }

        public int getSslStoragePort() {
            return sslStoragePort;
        }

        public String getListenAddress() {
            return listenAddress;
        }

        public boolean isStartNativeTransport() {
            return startNativeTransport;
        }

        public int getNativeTransportPort() {
            return nativeTransportPort;
        }

        public boolean isStartRpc() {
            return startRpc;
        }

        public String getRpcAddress() {
            return rpcAddress;
        }

        public int getRpcPort() {
            return rpcPort;
        }

        public boolean isRpcKeepalive() {
            return rpcKeepalive;
        }

        public String getRpcServerType() {
            return rpcServerType;
        }

        public int getThriftFramedTransportSizeInMb() {
            return thriftFramedTransportSizeInMb;
        }

        public boolean isIncrementalBackups() {
            return incrementalBackups;
        }

        public boolean isSnapshotBeforeCompaction() {
            return snapshotBeforeCompaction;
        }

        public boolean isAutoSnapshot() {
            return autoSnapshot;
        }

        public int getTombstoneWarnThreshold() {
            return tombstoneWarnThreshold;
        }

        public int getTombstoneFailureThreshold() {
            return tombstoneFailureThreshold;
        }

        public int getColumnIndexSizeInKb() {
            return columnIndexSizeInKb;
        }

        public int getBatchSizeWarnThresholdInKb() {
            return batchSizeWarnThresholdInKb;
        }

        public int getBatchSizeFailThresholdInKb() {
            return batchSizeFailThresholdInKb;
        }

        public int getCompactionThroughputMbPerSec() {
            return compactionThroughputMbPerSec;
        }

        public int getCompactionLargePartitionWarningThresholdMb() {
            return compactionLargePartitionWarningThresholdMb;
        }

        public int getSstablePreemptiveOpenIntervalInMb() {
            return sstablePreemptiveOpenIntervalInMb;
        }

        public int getReadRequestTimeoutInMs() {
            return readRequestTimeoutInMs;
        }

        public int getRangeRequestTimeoutInMs() {
            return rangeRequestTimeoutInMs;
        }

        public int getWriteRequestTimeoutInMs() {
            return writeRequestTimeoutInMs;
        }

        public int getCounterWriteRequestTimeoutInMs() {
            return counterWriteRequestTimeoutInMs;
        }

        public int getCasContentionTimeoutInMs() {
            return casContentionTimeoutInMs;
        }

        public int getTruncateRequestTimeoutInMs() {
            return truncateRequestTimeoutInMs;
        }

        public int getRequestTimeoutInMs() {
            return requestTimeoutInMs;
        }

        public boolean isCrossNodeTimeout() {
            return crossNodeTimeout;
        }

        public String getEndpointSnitch() {
            return endpointSnitch;
        }

        public int getDynamicSnitchUpdateIntervalInMs() {
            return dynamicSnitchUpdateIntervalInMs;
        }

        public int getDynamicSnitchResetIntervalInMs() {
            return dynamicSnitchResetIntervalInMs;
        }

        public double getDynamicSnitchBadnessThreshold() {
            return dynamicSnitchBadnessThreshold;
        }

        public String getRequestScheduler() {
            return requestScheduler;
        }

        public String getInternodeCompression() {
            return internodeCompression;
        }

        public boolean isInterDcTcpNodelay() {
            return interDcTcpNodelay;
        }

        public int getTracetypeQueryTtl() {
            return tracetypeQueryTtl;
        }

        public int getTracetypeRepairTtl() {
            return tracetypeRepairTtl;
        }

        public boolean isEnableUserDefinedFunctions() {
            return enableUserDefinedFunctions;
        }

        public int getWindowsTimerInterval() {
            return windowsTimerInterval;
        }

        public Builder setClusterName(String clusterName) {
            this.clusterName = clusterName;
            return this;
        }

        public Builder setNumTokens(int numTokens) {
            this.numTokens = numTokens;
            return this;
        }

        public Builder setHintedHandoffEnabled(boolean hintedHandoffEnabled) {
            this.hintedHandoffEnabled = hintedHandoffEnabled;
            return this;
        }

        public Builder setMaxHintWindowInMs(int maxHintWindowInMs) {
            this.maxHintWindowInMs = maxHintWindowInMs;
            return this;
        }

        public Builder setHintedHandoffThrottleInKb(int hintedHandoffThrottleInKb) {
            this.hintedHandoffThrottleInKb = hintedHandoffThrottleInKb;
            return this;
        }

        public Builder setMaxHintsDeliveryThreads(int maxHintsDeliveryThreads) {
            this.maxHintsDeliveryThreads = maxHintsDeliveryThreads;
            return this;
        }

        public Builder setBatchlogReplayThrottleInKb(int batchlogReplayThrottleInKb) {
            this.batchlogReplayThrottleInKb = batchlogReplayThrottleInKb;
            return this;
        }

        public Builder setAuthenticator(String authenticator) {
            this.authenticator = authenticator;
            return this;
        }

        public Builder setAuthorizer(String authorizer) {
            this.authorizer = authorizer;
            return this;
        }

        public Builder setRoleManager(String roleManager) {
            this.roleManager = roleManager;
            return this;
        }

        public Builder setRolesValidityInMs(int rolesValidityInMs) {
            this.rolesValidityInMs = rolesValidityInMs;
            return this;
        }

        public Builder setPermissionsValidityInMs(int permissionsValidityInMs) {
            this.permissionsValidityInMs = permissionsValidityInMs;
            return this;
        }

        public Builder setPartitioner(String partitioner) {
            this.partitioner = partitioner;
            return this;
        }

        public Builder setPersistentVolume(String persistentVolume) {
            this.persistentVolume = persistentVolume;
            return this;
        }

        public Builder setDiskFailurePolicy(String diskFailurePolicy) {
            this.diskFailurePolicy = diskFailurePolicy;
            return this;
        }

        public Builder setCommitFailurePolicy(String commitFailurePolicy) {
            this.commitFailurePolicy = commitFailurePolicy;
            return this;
        }

        public Builder setKeyCacheSizeInMb(Integer keyCacheSizeInMb) {
            this.keyCacheSizeInMb = keyCacheSizeInMb;
            return this;
        }

        public Builder setKeyCacheSavePeriod(int keyCacheSavePeriod) {
            this.keyCacheSavePeriod = keyCacheSavePeriod;
            return this;
        }

        public Builder setRowCacheSizeInMb(int rowCacheSizeInMb) {
            this.rowCacheSizeInMb = rowCacheSizeInMb;
            return this;
        }

        public Builder setRowCacheSavePeriod(int rowCacheSavePeriod) {
            this.rowCacheSavePeriod = rowCacheSavePeriod;
            return this;
        }

        public Builder setCounterCacheSizeInMb(Integer counterCacheSizeInMb) {
            this.counterCacheSizeInMb = counterCacheSizeInMb;
            return this;
        }

        public Builder setCounterCacheSavePeriod(int counterCacheSavePeriod) {
            this.counterCacheSavePeriod = counterCacheSavePeriod;
            return this;
        }

        public Builder setCommitlogSync(String commitlogSync) {
            this.commitlogSync = commitlogSync;
            return this;
        }

        public Builder setCommitlogSyncPeriodInMs(int commitlogSyncPeriodInMs) {
            this.commitlogSyncPeriodInMs = commitlogSyncPeriodInMs;
            return this;
        }

        public Builder setCommitlogSegmentSizeInMb(int commitlogSegmentSizeInMb) {
            this.commitlogSegmentSizeInMb = commitlogSegmentSizeInMb;
            return this;
        }

        public Builder setSeedProvider(List<Map<String, Object>> seedProvider) {
            this.seedProvider = seedProvider;
            return this;
        }

        public Builder setConcurrentReads(int concurrentReads) {
            this.concurrentReads = concurrentReads;
            return this;
        }

        public Builder setConcurrentWrites(int concurrentWrites) {
            this.concurrentWrites = concurrentWrites;
            return this;
        }

        public Builder setConcurrentCounterWrites(int concurrentCounterWrites) {
            this.concurrentCounterWrites = concurrentCounterWrites;
            return this;
        }

        public Builder setMemtableAllocationType(String memtableAllocationType) {
            this.memtableAllocationType = memtableAllocationType;
            return this;
        }

        public Builder setIndexSummaryCapacityInMb(Integer indexSummaryCapacityInMb) {
            this.indexSummaryCapacityInMb = indexSummaryCapacityInMb;
            return this;
        }

        public Builder setIndexSummaryResizeIntervalInMinutes(int indexSummaryResizeIntervalInMinutes) {
            this.indexSummaryResizeIntervalInMinutes = indexSummaryResizeIntervalInMinutes;
            return this;
        }

        public Builder setTrickleFsync(boolean trickleFsync) {
            this.trickleFsync = trickleFsync;
            return this;
        }

        public Builder setTrickleFsyncIntervalInKb(int trickleFsyncIntervalInKb) {
            this.trickleFsyncIntervalInKb = trickleFsyncIntervalInKb;
            return this;
        }

        public Builder setStoragePort(int storagePort) {
            this.storagePort = storagePort;
            return this;
        }

        public Builder setSslStoragePort(int sslStoragePort) {
            this.sslStoragePort = sslStoragePort;
            return this;
        }

        public Builder setListenAddress(String listenAddress) {
            this.listenAddress = listenAddress;
            return this;
        }

        public Builder setStartNativeTransport(boolean startNativeTransport) {
            this.startNativeTransport = startNativeTransport;
            return this;
        }

        public Builder setNativeTransportPort(int nativeTransportPort) {
            this.nativeTransportPort = nativeTransportPort;
            return this;
        }

        public Builder setStartRpc(boolean startRpc) {
            this.startRpc = startRpc;
            return this;
        }

        public Builder setRpcAddress(String rpcAddress) {
            this.rpcAddress = rpcAddress;
            return this;
        }

        public Builder setRpcPort(int rpcPort) {
            this.rpcPort = rpcPort;
            return this;
        }

        public Builder setRpcKeepalive(boolean rpcKeepalive) {
            this.rpcKeepalive = rpcKeepalive;
            return this;
        }

        public Builder setRpcServerType(String rpcServerType) {
            this.rpcServerType = rpcServerType;
            return this;
        }

        public Builder setThriftFramedTransportSizeInMb(int thriftFramedTransportSizeInMb) {
            this.thriftFramedTransportSizeInMb = thriftFramedTransportSizeInMb;
            return this;
        }

        public Builder setIncrementalBackups(boolean incrementalBackups) {
            this.incrementalBackups = incrementalBackups;
            return this;
        }

        public Builder setSnapshotBeforeCompaction(boolean snapshotBeforeCompaction) {
            this.snapshotBeforeCompaction = snapshotBeforeCompaction;
            return this;
        }

        public Builder setAutoSnapshot(boolean autoSnapshot) {
            this.autoSnapshot = autoSnapshot;
            return this;
        }

        public Builder setTombstoneWarnThreshold(int tombstoneWarnThreshold) {
            this.tombstoneWarnThreshold = tombstoneWarnThreshold;
            return this;
        }

        public Builder setTombstoneFailureThreshold(int tombstoneFailureThreshold) {
            this.tombstoneFailureThreshold = tombstoneFailureThreshold;
            return this;
        }

        public Builder setColumnIndexSizeInKb(int columnIndexSizeInKb) {
            this.columnIndexSizeInKb = columnIndexSizeInKb;
            return this;
        }

        public Builder setBatchSizeWarnThresholdInKb(int batchSizeWarnThresholdInKb) {
            this.batchSizeWarnThresholdInKb = batchSizeWarnThresholdInKb;
            return this;
        }

        public Builder setBatchSizeFailThresholdInKb(int batchSizeFailThresholdInKb) {
            this.batchSizeFailThresholdInKb = batchSizeFailThresholdInKb;
            return this;
        }

        public Builder setCompactionThroughputMbPerSec(int compactionThroughputMbPerSec) {
            this.compactionThroughputMbPerSec = compactionThroughputMbPerSec;
            return this;
        }

        public Builder setCompactionLargePartitionWarningThresholdMb(int compactionLargePartitionWarningThresholdMb) {
            this.compactionLargePartitionWarningThresholdMb = compactionLargePartitionWarningThresholdMb;
            return this;
        }

        public Builder setSstablePreemptiveOpenIntervalInMb(int sstablePreemptiveOpenIntervalInMb) {
            this.sstablePreemptiveOpenIntervalInMb = sstablePreemptiveOpenIntervalInMb;
            return this;
        }

        public Builder setReadRequestTimeoutInMs(int readRequestTimeoutInMs) {
            this.readRequestTimeoutInMs = readRequestTimeoutInMs;
            return this;
        }

        public Builder setRangeRequestTimeoutInMs(int rangeRequestTimeoutInMs) {
            this.rangeRequestTimeoutInMs = rangeRequestTimeoutInMs;
            return this;
        }

        public Builder setWriteRequestTimeoutInMs(int writeRequestTimeoutInMs) {
            this.writeRequestTimeoutInMs = writeRequestTimeoutInMs;
            return this;
        }

        public Builder setCounterWriteRequestTimeoutInMs(int counterWriteRequestTimeoutInMs) {
            this.counterWriteRequestTimeoutInMs = counterWriteRequestTimeoutInMs;
            return this;
        }

        public Builder setCasContentionTimeoutInMs(int casContentionTimeoutInMs) {
            this.casContentionTimeoutInMs = casContentionTimeoutInMs;
            return this;
        }

        public Builder setTruncateRequestTimeoutInMs(int truncateRequestTimeoutInMs) {
            this.truncateRequestTimeoutInMs = truncateRequestTimeoutInMs;
            return this;
        }

        public Builder setRequestTimeoutInMs(int requestTimeoutInMs) {
            this.requestTimeoutInMs = requestTimeoutInMs;
            return this;
        }

        public Builder setCrossNodeTimeout(boolean crossNodeTimeout) {
            this.crossNodeTimeout = crossNodeTimeout;
            return this;
        }

        public Builder setEndpointSnitch(String endpointSnitch) {
            this.endpointSnitch = endpointSnitch;
            return this;
        }

        public Builder setDynamicSnitchUpdateIntervalInMs(int dynamicSnitchUpdateIntervalInMs) {
            this.dynamicSnitchUpdateIntervalInMs = dynamicSnitchUpdateIntervalInMs;
            return this;
        }

        public Builder setDynamicSnitchResetIntervalInMs(int dynamicSnitchResetIntervalInMs) {
            this.dynamicSnitchResetIntervalInMs = dynamicSnitchResetIntervalInMs;
            return this;
        }

        public Builder setDynamicSnitchBadnessThreshold(double dynamicSnitchBadnessThreshold) {
            this.dynamicSnitchBadnessThreshold = dynamicSnitchBadnessThreshold;
            return this;
        }

        public Builder setRequestScheduler(String requestScheduler) {
            this.requestScheduler = requestScheduler;
            return this;
        }

        public Builder setInternodeCompression(String internodeCompression) {
            this.internodeCompression = internodeCompression;
            return this;
        }

        public Builder setInterDcTcpNodelay(boolean interDcTcpNodelay) {
            this.interDcTcpNodelay = interDcTcpNodelay;
            return this;
        }

        public Builder setTracetypeQueryTtl(int tracetypeQueryTtl) {
            this.tracetypeQueryTtl = tracetypeQueryTtl;
            return this;
        }

        public Builder setTracetypeRepairTtl(int tracetypeRepairTtl) {
            this.tracetypeRepairTtl = tracetypeRepairTtl;
            return this;
        }

        public Builder setEnableUserDefinedFunctions(boolean enableUserDefinedFunctions) {
            this.enableUserDefinedFunctions = enableUserDefinedFunctions;
            return this;
        }

        public Builder setWindowsTimerInterval(int windowsTimerInterval) {
            this.windowsTimerInterval = windowsTimerInterval;
            return this;
        }

        public CassandraApplicationConfig build() {

            return create(clusterName,
                    numTokens,
                    hintedHandoffEnabled,
                    maxHintWindowInMs,
                    hintedHandoffThrottleInKb,
                    maxHintsDeliveryThreads,
                    batchlogReplayThrottleInKb,
                    authenticator,
                    authorizer,
                    roleManager,
                    rolesValidityInMs,
                    permissionsValidityInMs,
                    partitioner,
                    persistentVolume,
                    diskFailurePolicy,
                    commitFailurePolicy,
                    keyCacheSizeInMb,
                    keyCacheSavePeriod,
                    rowCacheSizeInMb,
                    rowCacheSavePeriod,
                    counterCacheSizeInMb,
                    counterCacheSavePeriod,
                    commitlogSync,
                    commitlogSyncPeriodInMs,
                    commitlogSegmentSizeInMb,
                    seedProvider,
                    concurrentReads,
                    concurrentWrites,
                    concurrentCounterWrites,
                    memtableAllocationType,
                    indexSummaryCapacityInMb,
                    indexSummaryResizeIntervalInMinutes,
                    trickleFsync,
                    trickleFsyncIntervalInKb,
                    storagePort,
                    sslStoragePort,
                    listenAddress,
                    startNativeTransport,
                    nativeTransportPort,
                    startRpc,
                    rpcAddress,
                    rpcPort,
                    rpcKeepalive,
                    rpcServerType,
                    thriftFramedTransportSizeInMb,
                    incrementalBackups,
                    snapshotBeforeCompaction,
                    autoSnapshot,
                    tombstoneWarnThreshold,
                    tombstoneFailureThreshold,
                    columnIndexSizeInKb,
                    batchSizeWarnThresholdInKb,
                    batchSizeFailThresholdInKb,
                    compactionThroughputMbPerSec,
                    compactionLargePartitionWarningThresholdMb,
                    sstablePreemptiveOpenIntervalInMb,
                    readRequestTimeoutInMs,
                    rangeRequestTimeoutInMs,
                    writeRequestTimeoutInMs,
                    counterWriteRequestTimeoutInMs,
                    casContentionTimeoutInMs,
                    truncateRequestTimeoutInMs,
                    requestTimeoutInMs,
                    crossNodeTimeout,
                    endpointSnitch,
                    dynamicSnitchUpdateIntervalInMs,
                    dynamicSnitchResetIntervalInMs,
                    dynamicSnitchBadnessThreshold,
                    requestScheduler,
                    internodeCompression,
                    interDcTcpNodelay,
                    tracetypeQueryTtl,
                    tracetypeRepairTtl,
                    enableUserDefinedFunctions,
                    windowsTimerInterval);
        }
    }


}
