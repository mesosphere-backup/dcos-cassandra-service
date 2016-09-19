package com.mesosphere.dcos.cassandra.scheduler;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraData;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraMode;
import org.apache.mesos.Protos;

import java.util.UUID;

public class TestUtils {
    public static final Protos.FrameworkID generateFrameworkId() {
        return Protos.FrameworkID
                .newBuilder()
                .setValue(UUID.randomUUID().toString())
                .build();
    }

    public static final Protos.MasterInfo generateMasterInfo() {
        return Protos.MasterInfo.newBuilder()
                .setId(UUID.randomUUID().toString())
                .setIp(ipToInt("127.0.0.1"))
                .setPort(5050)
                .build();
    }

    public static int ipToInt(String ipAddress) {
        String[] ipAddressInArray = ipAddress.split("\\.");

        int result = 0;
        for (int i = 0; i < ipAddressInArray.length; i++) {

            int power = 3 - i;
            int ip = Integer.parseInt(ipAddressInArray[i]);
            result += ip * Math.pow(256, power);

        }

        return result;
    }

    public static Protos.Offer generateOffer(
            String frameworkId,
            double cpu,
            int memory,
            int disk) {
        final String offerUUID = UUID.randomUUID().toString();
        return generateOffer(frameworkId, cpu, memory, disk, offerUUID, offerUUID);
    }

    public static Protos.Offer generateOffer(
            String frameworkId,
            double cpu,
            int memory,
            int disk,
            String slaveId,
            String offerUUID) {
        Protos.Resource cpuRes = Protos.Resource.newBuilder()
                .setName("cpus")
                .setType(Protos.Value.Type.SCALAR)
                .setRole("*")
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(cpu))
                .build();
        Protos.Resource memRes = Protos.Resource.newBuilder()
                .setName("mem")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(memory))
                .build();
        Protos.Resource diskRes = Protos.Resource.newBuilder()
                .setName("disk")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(disk))
                .build();
        final Protos.Value.Range portRange = Protos.Value.Range.newBuilder()
                .setBegin(5000)
                .setEnd(40000).build();
        Protos.Resource portsRes = Protos.Resource.newBuilder()
                .setName("ports")
                .setType(Protos.Value.Type.RANGES)
                .setRanges(Protos.Value.Ranges.newBuilder().addRange(portRange))
                .build();
        return Protos.Offer
                .newBuilder()
                .setId(Protos.OfferID.newBuilder().setValue(offerUUID))
                .setFrameworkId(Protos.FrameworkID.newBuilder().setValue(frameworkId))
                .setSlaveId(Protos.SlaveID.newBuilder().setValue(slaveId))
                .setHostname("127.0.0.1")
                .addResources(cpuRes)
                .addResources(memRes)
                .addResources(diskRes)
                .addResources(portsRes)
                .build();
    }

    public static Protos.Offer generateReplacementOffer(
            String frameworkId,
            Protos.TaskInfo taskInfo,
            Protos.TaskInfo templateTaskInfo) {
        final String offerUUID = UUID.randomUUID().toString();
        return Protos.Offer
                .newBuilder()
                .setId(Protos.OfferID.newBuilder().setValue(offerUUID))
                .setFrameworkId(Protos.FrameworkID.newBuilder().setValue(frameworkId))
                .setSlaveId(Protos.SlaveID.newBuilder().setValue(taskInfo.getSlaveId().getValue()))
                .setHostname("127.0.0.1")
                .addAllResources(taskInfo.getResourcesList())
                .addAllResources(taskInfo.getExecutor().getResourcesList())
                .addAllResources(templateTaskInfo.getResourcesList())
                .build();
    }

    public static Protos.Offer generateUpdateOffer(
            String frameworkId,
            Protos.TaskInfo taskInfo,
            Protos.TaskInfo templateTaskInfo,
            double cpu,
            int memory,
            int disk) {
        final String offerUUID = UUID.randomUUID().toString();
        Protos.Resource cpuRes = Protos.Resource.newBuilder()
                .setName("cpus")
                .setType(Protos.Value.Type.SCALAR)
                .setRole("*")
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(cpu))
                .build();
        Protos.Resource memRes = Protos.Resource.newBuilder()
                .setName("mem")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(memory))
                .build();
        Protos.Resource diskRes = Protos.Resource.newBuilder()
                .setName("disk")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(disk))
                .build();
        return Protos.Offer
                .newBuilder()
                .setId(Protos.OfferID.newBuilder().setValue(offerUUID))
                .setFrameworkId(Protos.FrameworkID.newBuilder().setValue(frameworkId))
                .setSlaveId(Protos.SlaveID.newBuilder().setValue(taskInfo.getSlaveId().getValue()))
                .setHostname("127.0.0.1")
                .addAllResources(taskInfo.getResourcesList())
                .addAllResources(taskInfo.getExecutor().getResourcesList())
                .addAllResources(templateTaskInfo.getResourcesList())
                .addResources(cpuRes)
                .addResources(memRes)
                .addResources(diskRes)
                .build();
    }

    public static Protos.TaskStatus generateStatus(
            Protos.TaskID taskID,
            Protos.TaskState taskState) {
        return Protos.TaskStatus.newBuilder()
                .setTaskId(taskID)
                .setState(taskState)
                .build();
    }

    public static Protos.TaskStatus generateStatus(
            Protos.TaskID taskID,
            Protos.TaskState taskState,
            CassandraMode cassandraMode) {
        return Protos.TaskStatus.newBuilder()
                .setTaskId(taskID)
                .setState(taskState)
                .setData(CassandraData.createDaemonStatusData(cassandraMode).getBytes())
                .build();
    }
}
