# Release Guide

## Requirements
- JDK 8
- Protobuf 2.5.0
- GNU Sed 4.2.2 + (for generating new apache-casssandra package using `build-cassandra-bin.bash`)

## Clone the repo
```
$ git clone git@github.com:mesosphere/dcos-cassandra-service.git
```

## Updating apache-cassandra binary package

We need to update the vanilla apache-cassandra binary package for following reasons:

1. Distribute DC/OS seed provider jar
2. Make JMX_PORT configurable
3. Copy reporter-config JARs into lib directory for metric reporting:

To generate a new apache-cassandra binary package:

1. Execute following

  ```bash
  $ ./build-cassandra-bin.bash
  ```
See [build-cassandra-bin.bash](https://github.com/mesosphere/dcos-cassandra-service/blob/master/build-cassandra-bin.bash) for more details on usage.
2. Upload the `/path/to/dcos-cassandra-service/cassandra-bin-tmp/apache-cassandra-A.B.C-bin-dcos.tar.gz` to the designated release location.
3. Update the universe's [resource.json](https://github.com/mesosphere/dcos-cassandra-service/blob/master/universe/resource.json) with new URL if necessary.

## Releasing DC/OS Cassandra service

1. Make necessary chanages to [dcos-cassandra-service/universe](https://github.com/mesosphere/dcos-cassandra-service/tree/master/universe)
2. Prepare RELEASE NOTES
3. Cut a tag with following format: `X.Y.Z-A.B.C`, where `X.Y.Z` is the version of service and `A.B.C` is the version of underlying Cassandra

  ```
  git tag X.Y.Z-A.B.C
  git push origin X.Y.Z-A.B.C
  ```

4. Trigger the [0-trigger-tag](https://jenkins.mesosphere.com/service/jenkins/view/Infinity/job/cassandra/job/0-trigger-tag/) job with the tag version created in step 3.
5. Once build job in step 4 finishes successfully, take the stub universe from the [1-build-linux](https://jenkins.mesosphere.com/service/jenkins/view/Infinity/job/cassandra/job/1-build-linux/) job 
6. Use version from step 3 and stub universe URL from step 5 to trigger [release-to-universe](https://jenkins.mesosphere.com/service/jenkins/view/Infinity/job/infinity-tools/job/release-to-universe/) job.
