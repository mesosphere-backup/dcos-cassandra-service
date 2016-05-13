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
package com.mesosphere.dcos.cassandra.scheduler.config;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.config.CassandraApplicationConfig;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.config.HeapConfig;
import com.mesosphere.dcos.cassandra.common.config.Location;
import com.mesosphere.dcos.cassandra.common.tasks.Volume;
import org.apache.mesos.offer.VolumeRequirement;

import static com.mesosphere.dcos.cassandra.common.config
        .CassandraApplicationConfig.*;

/**
 * ConfigParser parses a CassandraConfig from a yaml configuration file. It
 * is used to mediate construction of the CassandraConfig such that defaults
 * and overrides may be applied programatically. The original implementation
 * used only the defined configuration objects, but a product decision lead
 * to the introduction of this class in order to not expose certain
 * configurable items.
 * @TODO Investigate if this class is still nessecary and potentially remove it.
 */
public class CassandraConfigParser {

    @JsonProperty("cpus")
    private double cpus;
    @JsonProperty("memory_mb")
    private int memoryMb;
    @JsonProperty("disk_mb")
    private int diskMb;
    @JsonProperty("disk_type")
    private VolumeRequirement.VolumeType diskType;
    @JsonProperty("version")
    private String version;
    @JsonProperty(NUM_TOKENS_KEY)
    private int numTokens;
    @JsonProperty(HINTED_HANDOFF_ENABLED_KEY)
    private boolean hintedHandoffEnabled;
    @JsonProperty(MAX_HINT_WINDOW_IN_MS_KEY)
    private int maxHintWindowInMs;
    @JsonProperty(HINTED_HANDOFF_THROTTLE_IN_KB_KEY)
    private int hintedHandoffThrottleInKb;
    @JsonProperty(MAX_HINTS_DELIVERY_THREADS_KEY)
    private int maxHintsDeliveryThreads;
    @JsonProperty(BATCHLOG_REPLAY_THROTTLE_IN_KB_KEY)
    private int batchlogReplayThrottleInKb;
    @JsonProperty(PARTITIONER_KEY)
    private String partitioner;
    @JsonProperty("volume_size_mb")
    private int volumeSizeMb;
    @JsonProperty(DISK_FAILURE_POLICY_KEY)
    private String diskFailurePolicy;
    @JsonProperty(COMMIT_FAILURE_POLICY_KEY)
    private String commitFailurePolicy;
    @JsonProperty(KEY_CACHE_SIZE_IN_MB_KEY)
    private Integer keyCacheSizeInMb;
    @JsonProperty(KEY_CACHE_SAVE_PERIOD_KEY)
    private int keyCacheSavePeriod;
    @JsonProperty(ROW_CACHE_SIZE_IN_MB_KEY)
    private int rowCacheSizeInMb;
    @JsonProperty(ROW_CACHE_SAVE_PERIOD_KEY)
    private int rowCacheSavePeriod;
    @JsonProperty(COUNTER_CACHE_SIZE_IN_MB_KEY)
    private Integer counterCacheSizeInMb;
    @JsonProperty(COUNTER_CACHE_SAVE_PERIOD_KEY)
    private int counterCacheSavePeriod;
    @JsonProperty(COMMITLOG_SYNC_KEY)
    private String commitlogSync;
    @JsonProperty(COMMITLOG_SYNC_PERIOD_IN_MS_KEY)
    private int commitlogSyncPeriodInMs;
    @JsonProperty(COMMITLOG_SEGMENT_SIZE_IN_MB_KEY)
    private int commitlogSegmentSizeInMb;
    @JsonProperty(CONCURRENT_READS_KEY)
    private int concurrentReads;
    @JsonProperty(CONCURRENT_WRITES_KEY)
    private int concurrentWrites;
    @JsonProperty(CONCURRENT_COUNTER_WRITES_KEY)
    private int concurrentCounterWrites;
    @JsonProperty(MEMTABLE_ALLOCATION_TYPE_KEY)
    private String memtableAllocationType;
    @JsonProperty(INDEX_SUMMARY_CAPACITY_IN_MB_KEY)
    private Integer indexSummaryCapacityInMb;
    @JsonProperty(INDEX_SUMMARY_RESIZE_INTERVAL_IN_MINUTES_KEY)
    private int indexSummaryResizeIntervalInMinutes;
    @JsonProperty(TRICKLE_FSYNC_KEY)
    private boolean trickleFsync;
    @JsonProperty(TRICKLE_FSYNC_INTERVAL_IN_KB_KEY)
    private int trickleFsyncIntervalInKb;
    @JsonProperty(STORAGE_PORT_KEY)
    private int storagePort;
    @JsonProperty(SSL_STORAGE_PORT_KEY)
    private int sslStoragePort;
    @JsonProperty(START_NATIVE_TRANSPORT_KEY)
    private boolean startNativeTransport;
    @JsonProperty(NATIVE_TRANSPORT_PORT_KEY)
    private int nativeTransportPort;
    @JsonProperty(START_RPC_KEY)
    private boolean startRpc;
    @JsonProperty(RPC_PORT_KEY)
    private int rpcPort;
    @JsonProperty(RPC_KEEPALIVE_KEY)
    private boolean rpcKeepalive;
    @JsonProperty(RPC_SERVER_TYPE_KEY)
    private String rpcServerType;
    @JsonProperty(THRIFT_FRAMED_TRANSPORT_SIZE_IN_MB_KEY)
    private int thriftFramedTransportSizeInMb;
    @JsonProperty(TOMBSTONE_WARN_THRESHOLD_KEY)
    private int tombstoneWarnThreshold;
    @JsonProperty(TOMBSTONE_FAILURE_THRESHOLD_KEY)
    private int tombstoneFailureThreshold;
    @JsonProperty(COLUMN_INDEX_SIZE_IN_KB_KEY)
    private int columnIndexSizeInKb;
    @JsonProperty(BATCH_SIZE_WARN_THRESHOLD_IN_KB_KEY)
    private int batchSizeWarnThresholdInKb;
    @JsonProperty(BATCH_SIZE_FAIL_THRESHOLD_IN_KB_KEY)
    private int batchSizeFailThresholdInKb;
    @JsonProperty(COMPACTION_THROUGHPUT_MB_PER_SEC_KEY)
    private int compactionThroughputMbPerSec;
    @JsonProperty(COMPACTION_LARGE_PARTITION_WARNING_THRESHOLD_MB_KEY)
    private int compactionLargePartitionWarningThresholdMb;
    @JsonProperty(SSTABLE_PREEMPTIVE_OPEN_INTERVAL_IN_MB_KEY)
    private int sstablePreemptiveOpenIntervalInMb;
    @JsonProperty(READ_REQUEST_TIMEOUT_IN_MS_KEY)
    private int readRequestTimeoutInMs;
    @JsonProperty(RANGE_REQUEST_TIMEOUT_IN_MS_KEY)
    private int rangeRequestTimeoutInMs;
    @JsonProperty(WRITE_REQUEST_TIMEOUT_IN_MS_KEY)
    private int writeRequestTimeoutInMs;
    @JsonProperty(COUNTER_WRITE_REQUEST_TIMEOUT_IN_MS_KEY)
    private int counterWriteRequestTimeoutInMs;
    @JsonProperty(CAS_CONTENTION_TIMEOUT_IN_MS_KEY)
    private int casContentionTimeoutInMs;
    @JsonProperty(TRUNCATE_REQUEST_TIMEOUT_IN_MS_KEY)
    private int truncateRequestTimeoutInMs;
    @JsonProperty(REQUEST_TIMEOUT_IN_MS_KEY)
    private int requestTimeoutInMs;
    @JsonProperty(CROSS_NODE_TIMEOUT_KEY)
    private boolean crossNodeTimeout;
    @JsonProperty(DYNAMIC_SNITCH_UPDATE_INTERVAL_IN_MS_KEY)
    private int dynamicSnitchUpdateIntervalInMs;
    @JsonProperty(DYNAMIC_SNITCH_RESET_INTERVAL_IN_MS_KEY)
    private int dynamicSnitchResetIntervalInMs;
    @JsonProperty(DYNAMIC_SNITCH_BADNESS_THRESHOLD_KEY)
    private double dynamicSnitchBadnessThreshold;
    @JsonProperty(REQUEST_SCHEDULER_KEY)
    private String requestScheduler;
    @JsonProperty(INTERNODE_COMPRESSION_KEY)
    private String internodeCompression;
    @JsonProperty(INTER_DC_TCP_NODELAY_KEY)
    private boolean interDcTcpNodelay;
    @JsonProperty(TRACETYPE_QUERY_TTL_KEY)
    private int tracetypeQueryTtl;
    @JsonProperty(TRACETYPE_REPAIR_TTL_KEY)
    private int tracetypeRepairTtl;
    @JsonProperty(WINDOWS_TIMER_INTERVAL_KEY)
    private int windowsTimerInterval;
    @JsonProperty("heap")
    private HeapConfig heap;
    @JsonProperty("jmx_port")
    private int jmxPort;
    @JsonProperty("location")
    private Location location;

    public CassandraConfigParser() {
        cpus = CassandraConfig.DEFAULT.getCpus();
        diskMb = CassandraConfig.DEFAULT.getDiskMb();
        diskType = CassandraConfig.DEFAULT.getDiskType();
        memoryMb = CassandraConfig.DEFAULT.getMemoryMb();
        version = CassandraConfig.DEFAULT.getVersion();
        numTokens = DEFAULT_NUM_TOKENS;
        hintedHandoffEnabled = DEFAULT_HINTED_HANDOFF_ENABLED;
        maxHintWindowInMs = DEFAULT_MAX_HINT_WINDOW_IN_MS;
        hintedHandoffThrottleInKb = DEFAULT_HINTED_HANDOFF_THROTTLE_IN_KB;
        maxHintsDeliveryThreads = DEFAULT_MAX_HINTS_DELIVERY_THREADS;
        batchlogReplayThrottleInKb = DEFAULT_BATCHLOG_REPLAY_THROTTLE_IN_KB;
        partitioner = DEFAULT_PARTITIONER;
        volumeSizeMb = CassandraConfig.DEFAULT.getVolume().getSizeMb();
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
        startNativeTransport = DEFAULT_START_NATIVE_TRANSPORT;
        nativeTransportPort = DEFAULT_NATIVE_TRANSPORT_PORT;
        startRpc = DEFAULT_START_RPC;
        rpcPort = DEFAULT_RPC_PORT;
        rpcKeepalive = DEFAULT_RPC_KEEPALIVE;
        rpcServerType = DEFAULT_RPC_SERVER_TYPE;
        thriftFramedTransportSizeInMb = DEFAULT_THRIFT_FRAMED_TRANSPORT_SIZE_IN_MB;
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
        dynamicSnitchUpdateIntervalInMs = DEFAULT_DYNAMIC_SNITCH_UPDATE_INTERVAL_IN_MS;
        dynamicSnitchResetIntervalInMs = DEFAULT_DYNAMIC_SNITCH_RESET_INTERVAL_IN_MS;
        dynamicSnitchBadnessThreshold = DEFAULT_DYNAMIC_SNITCH_BADNESS_THRESHOLD;
        requestScheduler = DEFAULT_REQUEST_SCHEDULER;
        internodeCompression = DEFAULT_INTERNODE_COMPRESSION;
        interDcTcpNodelay = DEFAULT_INTER_DC_TCP_NODELAY;
        tracetypeQueryTtl = DEFAULT_TRACETYPE_QUERY_TTL;
        tracetypeRepairTtl = DEFAULT_TRACETYPE_REPAIR_TTL;
        windowsTimerInterval = DEFAULT_WINDOWS_TIMER_INTERVAL;
    }

    private CassandraApplicationConfig getApplicationConfig(
            final String name,
            final String seedsUrl
    ) {

        return create(name,
                numTokens,
                hintedHandoffEnabled,
                maxHintWindowInMs,
                hintedHandoffThrottleInKb,
                maxHintsDeliveryThreads,
                batchlogReplayThrottleInKb,
                DEFAULT_AUTHENTICATOR,
                DEFAULT_AUTHORIZER,
                DEFAULT_ROLE_MANAGER,
                DEFAULT_ROLES_VALIDITY_IN_MS,
                DEFAULT_PERMISSIONS_VALIDITY_IN_MS,
                partitioner,
                CassandraConfig.DEFAULT.getVolume().getPath(),
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
                CassandraApplicationConfig.createDcosSeedProvider(seedsUrl),
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
                DEFAULT_LISTEN_ADDRESS,
                startNativeTransport,
                nativeTransportPort,
                startRpc,
                DEFAULT_RPC_ADDRESS,
                rpcPort,
                rpcKeepalive,
                rpcServerType,
                thriftFramedTransportSizeInMb,
                false,
                false,
                false,
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
                DEFAULT_ENDPOINT_SNITCH,
                dynamicSnitchUpdateIntervalInMs,
                dynamicSnitchResetIntervalInMs,
                dynamicSnitchBadnessThreshold,
                requestScheduler,
                internodeCompression,
                interDcTcpNodelay,
                tracetypeQueryTtl,
                tracetypeRepairTtl,
                false,
                windowsTimerInterval);
    }

    public CassandraConfig getCassandraConfig(
            final String name,
            final String seedsUrl
    ) {

        return CassandraConfig.create(
                version,
                cpus,
                memoryMb,
                diskMb,
                diskType,
                "",
                heap,
                location,
                jmxPort,
                Volume.create(
                        CassandraConfig.DEFAULT.getVolume().getPath(),
                        volumeSizeMb,
                        ""),
                getApplicationConfig(name, seedsUrl));
    }

    public int getBatchlogReplayThrottleInKb() {
        return batchlogReplayThrottleInKb;
    }

    public void setBatchlogReplayThrottleInKb(int batchlogReplayThrottleInKb) {
        this.batchlogReplayThrottleInKb = batchlogReplayThrottleInKb;
    }

    public int getBatchSizeFailThresholdInKb() {
        return batchSizeFailThresholdInKb;
    }

    public void setBatchSizeFailThresholdInKb(int batchSizeFailThresholdInKb) {
        this.batchSizeFailThresholdInKb = batchSizeFailThresholdInKb;
    }

    public int getBatchSizeWarnThresholdInKb() {
        return batchSizeWarnThresholdInKb;
    }

    public void setBatchSizeWarnThresholdInKb(int batchSizeWarnThresholdInKb) {
        this.batchSizeWarnThresholdInKb = batchSizeWarnThresholdInKb;
    }

    public int getCasContentionTimeoutInMs() {
        return casContentionTimeoutInMs;
    }

    public void setCasContentionTimeoutInMs(int casContentionTimeoutInMs) {
        this.casContentionTimeoutInMs = casContentionTimeoutInMs;
    }

    public int getColumnIndexSizeInKb() {
        return columnIndexSizeInKb;
    }

    public void setColumnIndexSizeInKb(int columnIndexSizeInKb) {
        this.columnIndexSizeInKb = columnIndexSizeInKb;
    }

    public String getCommitFailurePolicy() {
        return commitFailurePolicy;
    }

    public void setCommitFailurePolicy(String commitFailurePolicy) {
        this.commitFailurePolicy = commitFailurePolicy;
    }

    public int getCommitlogSegmentSizeInMb() {
        return commitlogSegmentSizeInMb;
    }

    public void setCommitlogSegmentSizeInMb(int commitlogSegmentSizeInMb) {
        this.commitlogSegmentSizeInMb = commitlogSegmentSizeInMb;
    }

    public String getCommitlogSync() {
        return commitlogSync;
    }

    public void setCommitlogSync(String commitlogSync) {
        this.commitlogSync = commitlogSync;
    }

    public int getCommitlogSyncPeriodInMs() {
        return commitlogSyncPeriodInMs;
    }

    public void setCommitlogSyncPeriodInMs(int commitlogSyncPeriodInMs) {
        this.commitlogSyncPeriodInMs = commitlogSyncPeriodInMs;
    }

    public int getCompactionLargePartitionWarningThresholdMb() {
        return compactionLargePartitionWarningThresholdMb;
    }

    public void setCompactionLargePartitionWarningThresholdMb(int compactionLargePartitionWarningThresholdMb) {
        this.compactionLargePartitionWarningThresholdMb = compactionLargePartitionWarningThresholdMb;
    }

    public int getCompactionThroughputMbPerSec() {
        return compactionThroughputMbPerSec;
    }

    public void setCompactionThroughputMbPerSec(int compactionThroughputMbPerSec) {
        this.compactionThroughputMbPerSec = compactionThroughputMbPerSec;
    }

    public int getConcurrentCounterWrites() {
        return concurrentCounterWrites;
    }

    public void setConcurrentCounterWrites(int concurrentCounterWrites) {
        this.concurrentCounterWrites = concurrentCounterWrites;
    }

    public int getConcurrentReads() {
        return concurrentReads;
    }

    public void setConcurrentReads(int concurrentReads) {
        this.concurrentReads = concurrentReads;
    }

    public int getConcurrentWrites() {
        return concurrentWrites;
    }

    public void setConcurrentWrites(int concurrentWrites) {
        this.concurrentWrites = concurrentWrites;
    }

    public int getCounterCacheSavePeriod() {
        return counterCacheSavePeriod;
    }

    public void setCounterCacheSavePeriod(int counterCacheSavePeriod) {
        this.counterCacheSavePeriod = counterCacheSavePeriod;
    }

    public Integer getCounterCacheSizeInMb() {
        return counterCacheSizeInMb;
    }

    public void setCounterCacheSizeInMb(Integer counterCacheSizeInMb) {
        this.counterCacheSizeInMb = counterCacheSizeInMb;
    }

    public int getCounterWriteRequestTimeoutInMs() {
        return counterWriteRequestTimeoutInMs;
    }

    public void setCounterWriteRequestTimeoutInMs(int counterWriteRequestTimeoutInMs) {
        this.counterWriteRequestTimeoutInMs = counterWriteRequestTimeoutInMs;
    }

    public double getCpus() {
        return cpus;
    }

    public void setCpus(double cpus) {
        this.cpus = cpus;
    }

    public boolean isCrossNodeTimeout() {
        return crossNodeTimeout;
    }

    public void setCrossNodeTimeout(boolean crossNodeTimeout) {
        this.crossNodeTimeout = crossNodeTimeout;
    }

    public String getDiskFailurePolicy() {
        return diskFailurePolicy;
    }

    public void setDiskFailurePolicy(String diskFailurePolicy) {
        this.diskFailurePolicy = diskFailurePolicy;
    }

    public int getDiskMb() {
        return diskMb;
    }

    public void setDiskMb(int diskMb) {
        this.diskMb = diskMb;
    }

    public double getDynamicSnitchBadnessThreshold() {
        return dynamicSnitchBadnessThreshold;
    }

    public void setDynamicSnitchBadnessThreshold(double dynamicSnitchBadnessThreshold) {
        this.dynamicSnitchBadnessThreshold = dynamicSnitchBadnessThreshold;
    }

    public int getDynamicSnitchResetIntervalInMs() {
        return dynamicSnitchResetIntervalInMs;
    }

    public void setDynamicSnitchResetIntervalInMs(int dynamicSnitchResetIntervalInMs) {
        this.dynamicSnitchResetIntervalInMs = dynamicSnitchResetIntervalInMs;
    }

    public int getDynamicSnitchUpdateIntervalInMs() {
        return dynamicSnitchUpdateIntervalInMs;
    }

    public void setDynamicSnitchUpdateIntervalInMs(int dynamicSnitchUpdateIntervalInMs) {
        this.dynamicSnitchUpdateIntervalInMs = dynamicSnitchUpdateIntervalInMs;
    }

    public HeapConfig getHeap() {
        return heap;
    }

    public void setHeap(HeapConfig heap) {
        this.heap = heap;
    }

    public boolean isHintedHandoffEnabled() {
        return hintedHandoffEnabled;
    }

    public void setHintedHandoffEnabled(boolean hintedHandoffEnabled) {
        this.hintedHandoffEnabled = hintedHandoffEnabled;
    }

    public int getHintedHandoffThrottleInKb() {
        return hintedHandoffThrottleInKb;
    }

    public void setHintedHandoffThrottleInKb(int hintedHandoffThrottleInKb) {
        this.hintedHandoffThrottleInKb = hintedHandoffThrottleInKb;
    }

    public Integer getIndexSummaryCapacityInMb() {
        return indexSummaryCapacityInMb;
    }

    public void setIndexSummaryCapacityInMb(Integer indexSummaryCapacityInMb) {
        this.indexSummaryCapacityInMb = indexSummaryCapacityInMb;
    }

    public int getIndexSummaryResizeIntervalInMinutes() {
        return indexSummaryResizeIntervalInMinutes;
    }

    public void setIndexSummaryResizeIntervalInMinutes(int indexSummaryResizeIntervalInMinutes) {
        this.indexSummaryResizeIntervalInMinutes = indexSummaryResizeIntervalInMinutes;
    }

    public boolean isInterDcTcpNodelay() {
        return interDcTcpNodelay;
    }

    public void setInterDcTcpNodelay(boolean interDcTcpNodelay) {
        this.interDcTcpNodelay = interDcTcpNodelay;
    }

    public String getInternodeCompression() {
        return internodeCompression;
    }

    public void setInternodeCompression(String internodeCompression) {
        this.internodeCompression = internodeCompression;
    }

    public int getJmxPort() {
        return jmxPort;
    }

    public void setJmxPort(int jmxPort) {
        this.jmxPort = jmxPort;
    }

    public int getKeyCacheSavePeriod() {
        return keyCacheSavePeriod;
    }

    public void setKeyCacheSavePeriod(int keyCacheSavePeriod) {
        this.keyCacheSavePeriod = keyCacheSavePeriod;
    }

    public Integer getKeyCacheSizeInMb() {
        return keyCacheSizeInMb;
    }

    public void setKeyCacheSizeInMb(Integer keyCacheSizeInMb) {
        this.keyCacheSizeInMb = keyCacheSizeInMb;
    }

    public int getMaxHintsDeliveryThreads() {
        return maxHintsDeliveryThreads;
    }

    public void setMaxHintsDeliveryThreads(int maxHintsDeliveryThreads) {
        this.maxHintsDeliveryThreads = maxHintsDeliveryThreads;
    }

    public int getMaxHintWindowInMs() {
        return maxHintWindowInMs;
    }

    public void setMaxHintWindowInMs(int maxHintWindowInMs) {
        this.maxHintWindowInMs = maxHintWindowInMs;
    }

    public int getMemoryMb() {
        return memoryMb;
    }

    public void setMemoryMb(int memoryMb) {
        this.memoryMb = memoryMb;
    }

    public String getMemtableAllocationType() {
        return memtableAllocationType;
    }

    public void setMemtableAllocationType(String memtableAllocationType) {
        this.memtableAllocationType = memtableAllocationType;
    }

    public int getNativeTransportPort() {
        return nativeTransportPort;
    }

    public void setNativeTransportPort(int nativeTransportPort) {
        this.nativeTransportPort = nativeTransportPort;
    }

    public int getNumTokens() {
        return numTokens;
    }

    public void setNumTokens(int numTokens) {
        this.numTokens = numTokens;
    }

    public String getPartitioner() {
        return partitioner;
    }

    public void setPartitioner(String partitioner) {
        this.partitioner = partitioner;
    }

    public int getVolumeSizeMb() {
        return volumeSizeMb;
    }

    public void setVolumeSizeMb(int volumeSizeMb) {
        this.volumeSizeMb = volumeSizeMb;
    }

    public int getRangeRequestTimeoutInMs() {
        return rangeRequestTimeoutInMs;
    }

    public void setRangeRequestTimeoutInMs(int rangeRequestTimeoutInMs) {
        this.rangeRequestTimeoutInMs = rangeRequestTimeoutInMs;
    }

    public int getReadRequestTimeoutInMs() {
        return readRequestTimeoutInMs;
    }

    public void setReadRequestTimeoutInMs(int readRequestTimeoutInMs) {
        this.readRequestTimeoutInMs = readRequestTimeoutInMs;
    }

    public String getRequestScheduler() {
        return requestScheduler;
    }

    public void setRequestScheduler(String requestScheduler) {
        this.requestScheduler = requestScheduler;
    }

    public int getRequestTimeoutInMs() {
        return requestTimeoutInMs;
    }

    public void setRequestTimeoutInMs(int requestTimeoutInMs) {
        this.requestTimeoutInMs = requestTimeoutInMs;
    }

    public int getRowCacheSavePeriod() {
        return rowCacheSavePeriod;
    }

    public void setRowCacheSavePeriod(int rowCacheSavePeriod) {
        this.rowCacheSavePeriod = rowCacheSavePeriod;
    }

    public int getRowCacheSizeInMb() {
        return rowCacheSizeInMb;
    }

    public void setRowCacheSizeInMb(int rowCacheSizeInMb) {
        this.rowCacheSizeInMb = rowCacheSizeInMb;
    }

    public boolean isRpcKeepalive() {
        return rpcKeepalive;
    }

    public void setRpcKeepalive(boolean rpcKeepalive) {
        this.rpcKeepalive = rpcKeepalive;
    }

    public int getRpcPort() {
        return rpcPort;
    }

    public void setRpcPort(int rpcPort) {
        this.rpcPort = rpcPort;
    }

    public String getRpcServerType() {
        return rpcServerType;
    }

    public void setRpcServerType(String rpcServerType) {
        this.rpcServerType = rpcServerType;
    }

    public int getSslStoragePort() {
        return sslStoragePort;
    }

    public void setSslStoragePort(int sslStoragePort) {
        this.sslStoragePort = sslStoragePort;
    }

    public int getSstablePreemptiveOpenIntervalInMb() {
        return sstablePreemptiveOpenIntervalInMb;
    }

    public void setSstablePreemptiveOpenIntervalInMb(int sstablePreemptiveOpenIntervalInMb) {
        this.sstablePreemptiveOpenIntervalInMb = sstablePreemptiveOpenIntervalInMb;
    }

    public boolean isStartNativeTransport() {
        return startNativeTransport;
    }

    public void setStartNativeTransport(boolean startNativeTransport) {
        this.startNativeTransport = startNativeTransport;
    }

    public boolean isStartRpc() {
        return startRpc;
    }

    public void setStartRpc(boolean startRpc) {
        this.startRpc = startRpc;
    }

    public int getStoragePort() {
        return storagePort;
    }

    public void setStoragePort(int storagePort) {
        this.storagePort = storagePort;
    }

    public int getThriftFramedTransportSizeInMb() {
        return thriftFramedTransportSizeInMb;
    }

    public void setThriftFramedTransportSizeInMb(int thriftFramedTransportSizeInMb) {
        this.thriftFramedTransportSizeInMb = thriftFramedTransportSizeInMb;
    }

    public int getTombstoneFailureThreshold() {
        return tombstoneFailureThreshold;
    }

    public void setTombstoneFailureThreshold(int tombstoneFailureThreshold) {
        this.tombstoneFailureThreshold = tombstoneFailureThreshold;
    }

    public int getTombstoneWarnThreshold() {
        return tombstoneWarnThreshold;
    }

    public void setTombstoneWarnThreshold(int tombstoneWarnThreshold) {
        this.tombstoneWarnThreshold = tombstoneWarnThreshold;
    }

    public int getTracetypeQueryTtl() {
        return tracetypeQueryTtl;
    }

    public void setTracetypeQueryTtl(int tracetypeQueryTtl) {
        this.tracetypeQueryTtl = tracetypeQueryTtl;
    }

    public int getTracetypeRepairTtl() {
        return tracetypeRepairTtl;
    }

    public void setTracetypeRepairTtl(int tracetypeRepairTtl) {
        this.tracetypeRepairTtl = tracetypeRepairTtl;
    }

    public boolean isTrickleFsync() {
        return trickleFsync;
    }

    public void setTrickleFsync(boolean trickleFsync) {
        this.trickleFsync = trickleFsync;
    }

    public int getTrickleFsyncIntervalInKb() {
        return trickleFsyncIntervalInKb;
    }

    public void setTrickleFsyncIntervalInKb(int trickleFsyncIntervalInKb) {
        this.trickleFsyncIntervalInKb = trickleFsyncIntervalInKb;
    }

    public int getTruncateRequestTimeoutInMs() {
        return truncateRequestTimeoutInMs;
    }

    public void setTruncateRequestTimeoutInMs(int truncateRequestTimeoutInMs) {
        this.truncateRequestTimeoutInMs = truncateRequestTimeoutInMs;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public int getWindowsTimerInterval() {
        return windowsTimerInterval;
    }

    public void setWindowsTimerInterval(int windowsTimerInterval) {
        this.windowsTimerInterval = windowsTimerInterval;
    }

    public int getWriteRequestTimeoutInMs() {
        return writeRequestTimeoutInMs;
    }

    public void setWriteRequestTimeoutInMs(int writeRequestTimeoutInMs) {
        this.writeRequestTimeoutInMs = writeRequestTimeoutInMs;
    }

    public Location getLocation() {
        return location;
    }

    public CassandraConfigParser setLocation(Location location) {
        this.location = location;
        return this;
    }
}
