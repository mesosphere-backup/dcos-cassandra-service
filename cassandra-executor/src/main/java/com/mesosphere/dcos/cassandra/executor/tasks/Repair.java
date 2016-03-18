package com.mesosphere.dcos.cassandra.executor.tasks;

import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairStatus;
import com.mesosphere.dcos.cassandra.common.tasks.repair.RepairTask;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.ExecutionException;


public class Repair implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Repair.class);

    private final NodeProbe probe;
    private final ExecutorDriver driver;
    private final RepairTask task;

    public Repair(final ExecutorDriver driver,
                  final NodeProbe probe,
                  final RepairTask task) {
        this.driver = driver;
        this.probe = probe;
        this.task = task;
    }

    private List<String> getKeySpaces(){
        if(task.getKeySpaces().isEmpty()){
            return probe.getNonSystemKeyspaces();
        } else {
            return task.getKeySpaces();
        }
    }

    private String [] getColumnFamilies(){
        if(task.getColumnFamilies().isEmpty()){
            return new String[0];
        } else{
            String [] cf = new String[task.getColumnFamilies().size()];
            return task.getColumnFamilies().toArray(cf);
        }
    }

    @Override
    public void run() {
        try {
            // Send TASK_RUNNING

            final List<String> keySpaces = getKeySpaces();
            final String [] columnFamilies = getColumnFamilies();
            sendStatus(driver, Protos.TaskState.TASK_RUNNING,
                    String.format("Starting repair: keySpaces = %s, " +
                            "columnFamilies = %s",
                            keySpaces,
                            Arrays.asList(columnFamilies)));

            for(String keyspace: keySpaces){
                LOGGER.info("Starting repair : keySpace = {}, " +
                        "columnFamilies = {}",
                        keyspace,
                        Arrays.asList(columnFamilies));

                Map<String, String> options = new HashMap<String, String>();
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                PrintStream outStream = new PrintStream(baos);
                probe.repairAsync(outStream, keyspace, options);
                LOGGER.info("Repair output = {}", outStream.toString());
                LOGGER.info("Completed repair : keySpace = {}, " +
                                "columnFamilies = {}",
                        keyspace,
                        Arrays.asList(columnFamilies));
            }


            // Send TASK_FINISHED
            sendStatus(driver, Protos.TaskState.TASK_FINISHED,
                    String.format("Completed repair: keySpaces = %s, " +
                                    "columnFamilies = %s",
                            keySpaces,
                            Arrays.asList(columnFamilies)));
        } catch (Exception e) {
            // Send TASK_FAILED
            final String errorMessage = "Repair Failed Reason: " + e;
            LOGGER.error(errorMessage);
            sendStatus(driver, Protos.TaskState.TASK_FAILED, errorMessage);
        }
    }

    private void sendStatus(ExecutorDriver driver,
                            Protos.TaskState state, String message) {
        Protos.TaskStatus status = RepairStatus.create(
                state,
                task.getId(),
                task.getSlaveId(),
                task.getExecutor().getId(),
                Optional.of(message)
        ).toProto();
        driver.sendStatusUpdate(status);
    }
}
