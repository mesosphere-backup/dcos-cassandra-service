package com.mesosphere.dcos.cassandra.common.util;

import org.apache.mesos.Protos.*;
import org.apache.mesos.offer.ResourceUtils;
import org.apache.mesos.util.Algorithms;

import java.util.*;
import java.util.stream.Collectors;

public class TaskUtils {

    public static final String CPUS = "cpus";
    public static final String MEM = "mem";
    public static final String DISK = "disk";
    public static final String PORTS = "ports";


    public static String createVolumeId() {
        return "volume-" + UUID.randomUUID();
    }

    public static Environment createEnvironment(
        final Map<String, String> env) {
        return Environment.newBuilder()
            .addAllVariables(env.entrySet().stream()
                .map(entry -> Environment.Variable.newBuilder()
                    .setName(entry.getKey())
                    .setValue(entry.getValue()).build())
                .collect(Collectors.toList()))
            .build();
    }

    public static Collection<CommandInfo.URI> createURIs(
        final Set<String> uris, boolean cacheFetchedUris) {
        return uris.stream()
            .map(uri -> CommandInfo.URI.newBuilder()
                .setCache(cacheFetchedUris)
                .setExecutable(false)
                .setExtract(true)
                .setValue(uri).build())
            .collect(Collectors.toList());
    }

    public static CommandInfo createCommandInfo(
        final String command,
        final List<String> arguments,
        final Set<String> uris,
        boolean cacheFetchedUris,
        final Map<String, String> environment) {
        return CommandInfo.newBuilder()
            .setValue(command)
            .addAllArguments(arguments)
            .addAllUris(createURIs(uris, cacheFetchedUris))
            .setEnvironment(createEnvironment(environment)).build();
    }

    public static Set<String> toSet(final Collection<CommandInfo.URI> uris) {
        return uris.stream()
            .map(uri -> uri.getValue())
            .collect(Collectors.toSet());
    }


    public static Map<String, String> toMap(final Environment environment) {
        return environment.getVariablesList().stream()
            .collect(
                Collectors.toMap(var -> var.getName(), var -> var.getValue()));
    }

    public static String getValue(final String name, final Environment env) {
        return env.getVariablesList().stream()
            .filter(var -> var.getName().equals(name))
            .findFirst()
            .map(var -> var.getValue())
            .orElse("");
    }

    public static boolean isCpus(final Resource resource) {
        return resource.hasName() && resource.getName().equalsIgnoreCase(CPUS);
    }

    public static boolean isMem(final Resource resource) {
        return resource.hasName() && resource.getName().equalsIgnoreCase(MEM);
    }

    public static boolean isDisk(final Resource resource) {
        return resource.hasName() && resource.getName().equalsIgnoreCase(DISK);
    }

    public static boolean isPorts(final Resource resource) {
        return resource.hasName() && resource.getName().equalsIgnoreCase(PORTS);
    }

    public static Resource createScalar(
        final String name,
        final double value,
        final String role,
        final String principal) {
        return Resource.newBuilder()
            .setName(name)
            .setRole(role)
            .setType(Value.Type.SCALAR)
            .setScalar(Value.Scalar.newBuilder().setValue(value))
            .setReservation(Resource.ReservationInfo.newBuilder()
                .setPrincipal(principal))
            .build();
    }

    private static Resource.DiskInfo createDiskInfo(
        final String path) {
        return Resource.DiskInfo.newBuilder()
            .setPersistence(Resource.DiskInfo.Persistence.newBuilder()
                .setId(createVolumeId()))
            .setVolume(Volume.newBuilder()
                .setContainerPath(path)
                .setMode(Volume.Mode.RW
                )).build();
    }

    public static List<Value.Range> createPortRanges(final Collection<Integer> ports) {
        return Algorithms.createRanges(ports);
    }

    public static Resource createRanges(
        final String name,
        final Collection<Value.Range> ranges,
        final String role,
        final String principal) {

        return Resource.newBuilder()
            .setType(Value.Type.RANGES)
            .setName(name)
            .setRanges(Value.Ranges.newBuilder().addAllRange(ranges))
            .setRole(role)
            .setReservation(Resource.ReservationInfo.newBuilder()
                .setPrincipal(principal)
                .build())
            .build();
    }

    public static Resource createCpus(
        double cpus,
        String role,
        String principal) {
        return ResourceUtils.getDesiredScalar(role, principal, CPUS, cpus);
    }

    public static Resource createMemoryMb(
        int memoryMb,
        String role,
        String principal) {
        return ResourceUtils.getDesiredScalar(role, principal, MEM, memoryMb);
    }

    public static Resource createPorts(
        final Collection<Integer> ports,
        final String role,
        final String principal) {
        return ResourceUtils.getDesiredRanges(role, principal, PORTS, Algorithms.createRanges(ports));
    }


    public static double getResourceCpus(final List<Resource> resources) {
        return resources.stream().filter(TaskUtils::isCpus)
            .map(resource -> resource.getScalar().getValue())
            .reduce(0.0, Double::sum);
    }


    public static int getResourceMemoryMb(final List<Resource> resources) {
        return (int) Math.floor(
            resources.stream().filter(TaskUtils::isCpus)
                .map(resource -> resource.getScalar().getValue())
                .reduce(0.0, Double::sum));
    }


    public static int getResourceDiskMb(final List<Resource> resources) {
        return (int) Math.floor(
            resources.stream().filter(TaskUtils::isCpus)
                .map(resource -> resource.getScalar().getValue())
                .reduce(0.0, Double::sum));
    }

    public static List<String> getVolumePaths(final List<Resource> resources) {
        return resources.stream().filter(resource ->
            isDisk(resource) &&
                resource.hasDisk() &&
                resource.getDisk().hasVolume()).map(
            resource -> resource.getDisk()
                .getVolume().getContainerPath())
            .collect(Collectors.toList());
    }

    public static TaskInfo copyVolumes(
        final TaskInfo source,
        final TaskInfo target) {

        List<Resource> sourceDisks = source.getResourcesList().stream()
            .filter(resource -> isDisk(resource) &&
                resource.hasDisk())
            .collect(Collectors.toList());

        List<Resource> minusDisks = target.getResourcesList().stream()
            .filter(resource -> !isDisk(resource))
            .collect(Collectors.toList());

        return TaskInfo.newBuilder(target)
            .clearResources()
            .addAllResources(minusDisks)
            .addAllResources(sourceDisks)
            .build();
    }

    public static List<Resource> updateResources(
        final double cpus,
        final int memoryMb,
        final List<Resource> resources) {
        return resources.stream()
            .map(resource -> {
                if (isCpus(resource)) {
                    return Resource.newBuilder(resource)
                        .setScalar(
                            Value.Scalar.newBuilder()
                                .setValue(cpus))
                        .build();
                } else if (isMem(resource)) {
                    return Resource.newBuilder(resource)
                        .setScalar(
                            Value.Scalar.newBuilder()
                                .setValue(memoryMb))
                        .build();
                } else {
                    return resource;
                }
            }).collect(Collectors.toList());
    }

    public static List<Resource> replaceDisks(final String path,
                                              final List<Resource> resources) {
        return resources.stream()
            .map(resource -> {
                if (isDisk(resource) && resource.hasDisk()) {
                    return Resource.newBuilder(resource)
                        .setDisk(createDiskInfo(path))
                        .build();
                } else {
                    return resource;
                }
            }).collect(Collectors.toList());
    }

}
