package com.mesosphere.dcos.cassandra.scheduler;

import com.mesosphere.dcos.cassandra.common.tasks.CassandraData;
import com.mesosphere.dcos.cassandra.common.tasks.CassandraMode;
import org.apache.mesos.Protos;
import org.apache.mesos.protobuf.ResourceBuilder;

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
        return Protos.Offer
                .newBuilder()
                .setId(Protos.OfferID.newBuilder().setValue(offerUUID))
                .setFrameworkId(Protos.FrameworkID.newBuilder().setValue(frameworkId))
                .setSlaveId(Protos.SlaveID.newBuilder().setValue(offerUUID))
                .setHostname("127.0.0.1")
                .addResources(ResourceBuilder.cpus(cpu))
                .addResources(ResourceBuilder.mem(memory))
                .addResources(ResourceBuilder.disk(disk))
                .addResources(ResourceBuilder.ports(5000, 40000))
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
