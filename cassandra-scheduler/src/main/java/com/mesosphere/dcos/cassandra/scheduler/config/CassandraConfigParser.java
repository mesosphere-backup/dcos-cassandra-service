package com.mesosphere.dcos.cassandra.scheduler.config;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.config.CassandraApplicationConfig;
import com.mesosphere.dcos.cassandra.common.config.CassandraConfig;
import com.mesosphere.dcos.cassandra.common.config.HeapConfig;

import java.util.Optional;

import static com.mesosphere.dcos.cassandra.common.config.CassandraApplicationConfig.*;

public class CassandraConfigParser {

    @JsonProperty("cpus")
    private double cpus;
    @JsonProperty("memoryMb")
    private int memoryMb;
    @JsonProperty("diskMb")
    private int diskMb;
    @JsonProperty("version")
    private String version;
    @JsonProperty("numTokens")
    private int numTokens;
    @JsonProperty("hintedHandoffEnabled")
    private boolean hintedHandoffEnabled;
    @JsonProperty("maxHintWindowInMs")
    private int maxHintWindowInMs;
    @JsonProperty("hintedHandoffThrottleInKb")
    private int hintedHandoffThrottleInKb;
    @JsonProperty("maxHintsDeliveryThreads")
    private int maxHintsDeliveryThreads;
    @JsonProperty("batchlogReplayThrottleInKb")
    private int batchlogReplayThrottleInKb;
    @JsonProperty("partitioner")
    private String partitioner;
    @JsonProperty("volumeSizeMb")
    private int volumeSizeMb;
    @JsonProperty("diskFailurePolicy")
    private String diskFailurePolicy;
    @JsonProperty("commitFailurePolicy")
    private String commitFailurePolicy;
    @JsonProperty("keyCacheSizeInMb")
    private Integer keyCacheSizeInMb;
    @JsonProperty("keyCacheSavePeriod")
    private int keyCacheSavePeriod;
    @JsonProperty("rowCacheSizeInMb")
    private int rowCacheSizeInMb;
    @JsonProperty("rowCacheSavePeriod")
    private int rowCacheSavePeriod;
    @JsonProperty("counterCacheSizeInMb")
    private Integer counterCacheSizeInMb;
    @JsonProperty("counterCacheSavePeriod")
    private int counterCacheSavePeriod;
    @JsonProperty("commitlogSync")
    private String commitlogSync;
    @JsonProperty("commitlogSyncPeriodInMs")
    private int commitlogSyncPeriodInMs;
    @JsonProperty("commitlogSegmentSizeInMb")
    private int commitlogSegmentSizeInMb;
    @JsonProperty("concurrentReads")
    private int concurrentReads;
    @JsonProperty("concurrentWrites")
    private int concurrentWrites;
    @JsonProperty("concurrentCounterWrites")
    private int concurrentCounterWrites;
    @JsonProperty("memtableAllocationType")
    private String memtableAllocationType;
    @JsonProperty("indexSummaryCapacityInMb")
    private Integer indexSummaryCapacityInMb;
    @JsonProperty("indexSummaryResizeIntervalInMinutes")
    private int indexSummaryResizeIntervalInMinutes;
    @JsonProperty("trickleFsync")
    private boolean trickleFsync;
    @JsonProperty("trickleFsyncIntervalInKb")
    private int trickleFsyncIntervalInKb;
    @JsonProperty("storagePort")
    private int storagePort;
    @JsonProperty("sslStoragePort")
    private int sslStoragePort;
    @JsonProperty("startNativeTransport")
    private boolean startNativeTransport;
    @JsonProperty("nativeTransportPort")
    private int nativeTransportPort;
    @JsonProperty("startRpc")
    private boolean startRpc;
    @JsonProperty("rpcPort")
    private int rpcPort;
    @JsonProperty("rpcKeepalive")
    private boolean rpcKeepalive;
    @JsonProperty("rpcServerType")
    private String rpcServerType;
    @JsonProperty("thriftFramedTransportSizeInMb")
    private int thriftFramedTransportSizeInMb;
    @JsonProperty("tombstoneWarnThreshold")
    private int tombstoneWarnThreshold;
    @JsonProperty("tombstoneFailureThreshold")
    private int tombstoneFailureThreshold;
    @JsonProperty("columnIndexSizeInKb")
    private int columnIndexSizeInKb;
    @JsonProperty("batchSizeWarnThresholdInKb")
    private int batchSizeWarnThresholdInKb;
    @JsonProperty("batchSizeFailThresholdInKb")
    private int batchSizeFailThresholdInKb;
    @JsonProperty("compactionThroughputMbPerSec")
    private int compactionThroughputMbPerSec;
    @JsonProperty("compactionLargePartitionWarningThresholdMb")
    private int compactionLargePartitionWarningThresholdMb;
    @JsonProperty("sstablePreemptiveOpenIntervalInMb")
    private int sstablePreemptiveOpenIntervalInMb;
    @JsonProperty("readRequestTimeoutInMs")
    private int readRequestTimeoutInMs;
    @JsonProperty("rangeRequestTimeoutInMs")
    private int rangeRequestTimeoutInMs;
    @JsonProperty("writeRequestTimeoutInMs")
    private int writeRequestTimeoutInMs;
    @JsonProperty("counterWriteRequestTimeoutInMs")
    private int counterWriteRequestTimeoutInMs;
    @JsonProperty("casContentionTimeoutInMs")
    private int casContentionTimeoutInMs;
    @JsonProperty("truncateRequestTimeoutInMs")
    private int truncateRequestTimeoutInMs;
    @JsonProperty("requestTimeoutInMs")
    private int requestTimeoutInMs;
    @JsonProperty("crossNodeTimeout")
    private boolean crossNodeTimeout;
    @JsonProperty("dynamicSnitchUpdateIntervalInMs")
    private int dynamicSnitchUpdateIntervalInMs;
    @JsonProperty("dynamicSnitchResetIntervalInMs")
    private int dynamicSnitchResetIntervalInMs;
    @JsonProperty("dynamicSnitchBadnessThreshold")
    private double dynamicSnitchBadnessThreshold;
    @JsonProperty("requestScheduler")
    private String requestScheduler;
    @JsonProperty("internodeCompression")
    private String internodeCompression;
    @JsonProperty("interDcTcpNodelay")
    private boolean interDcTcpNodelay;
    @JsonProperty("tracetypeQueryTtl")
    private int tracetypeQueryTtl;
    @JsonProperty("tracetypeRepairTtl")
    private int tracetypeRepairTtl;
    @JsonProperty("windowsTimerInterval")
    private int windowsTimerInterval;
    @JsonProperty("heap")
    private HeapConfig heap;
    @JsonProperty("jmxPort")
    private int jmxPort;

    public CassandraConfigParser() {
        cpus = CassandraConfig.DEFAULT.getCpus();
        diskMb = CassandraConfig.DEFAULT.getDiskMb();
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
                Optional.empty(),
                heap,
                CassandraConfig.DEFAULT.getLocation(),
                jmxPort,
                CassandraConfig.DEFAULT.getVolume(),
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

    public void setVolumeSizeMb(int persistentVolumeSizeMb) {
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
}
