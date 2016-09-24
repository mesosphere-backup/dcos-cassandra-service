# Dev Guide

## Requirements
- JDK 8
- Protobuf 2.5.0

## Clone the repo (including sub-modules)
```
$ git clone --recursive git@github.com:mesosphere/dcos-cassandra-service.git
```

## Build instructions
```
$ ./gradlew clean build
```

## Updating apache-cassandra binary package

We need to update the vanilla apache-cassandra binary package for following:
1. Make JMX_PORT configurable
2. Copy reporter-config JARs into lib directory for metric reporting:

See [build-cassandra-bin.bash](https://github.com/mesosphere/dcos-cassandra-service/blob/master/build-cassandra-bin.bash) for more details on usage.

```bash
$ ./build-cassandra-bin.bash
```
