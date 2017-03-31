# Release Guide

## Requirements
- JDK 8
- Protobuf 2.5.0
- GNU Sed 4.2.2 + (for generating new apache-casssandra package using `build-cassandra-bin.bash`)

## Clone the repo
```
$ git clone git@github.com:mesosphere/dcos-cassandra-service.git
```

# Apache Cassandra

## Updating apache-cassandra binary package

We need to update the vanilla apache-cassandra binary package for following reasons:

1. Distribute DC/OS seed provider jar
2. Make JMX_PORT configurable
3. Remove default `cassandra-rackdc.properties` and `/cassandra-topology.properties`
4. Copy reporter-config JARs into lib directory for metric reporting:

To generate a new apache-cassandra binary package:

- **Option A:** Jenkins-based build/upload

0. Make any needed changes to [build-cassandra-bin.bash](build-cassandra-bin.bash), eg version bumps.
1. Execute the [cassandra/build-upload-dist](https://jenkins.mesosphere.com/service/jenkins/view/Infinity/job/cassandra/job/build-upload-dist/) job in Jenkins. Note that this will refuse to overwrite a file that already exists, but that shouldn't be a problem if a new version of Cassandra is being built, in which case the new filename would be different.
2. Update the universe's [resource.json](universe/resource.json) with new URL if necessary.

- **Option B:** Manual build/upload

0. Make any needed changes to [build-cassandra-bin.bash](build-cassandra-bin.bash), eg version bumps.
1. Execute the following

  ```bash
  $ ./build-cassandra-bin.bash
  ```
See [build-cassandra-bin.bash](build-cassandra-bin.bash) for more details on usage.
2. Upload the `/path/to/dcos-cassandra-service/cassandra-bin-tmp/apache-cassandra-A.B.C-bin-dcos.tar.gz` to the designated release location.
3. Update the universe's [resource.json](universe/resource.json) with new URL if necessary.

## Releasing DC/OS Cassandra service

1. Make necessary chanages to [dcos-cassandra-service/universe](universe)
2. Prepare RELEASE NOTES
3. Cut a tag with following format: `X.Y.Z-A.B.C`, where `X.Y.Z` is the version of service and `A.B.C` is the version of underlying Cassandra

  ```
  git tag X.Y.Z-A.B.C
  git push origin X.Y.Z-A.B.C
  ```

4. Trigger the [0-trigger-tag](https://jenkins.mesosphere.com/service/jenkins/view/Infinity/job/cassandra/job/0-trigger-tag/) job with the tag version created in step 3.
5. Once build job in step 4 finishes successfully, take the stub universe from the [1-build-linux](https://jenkins.mesosphere.com/service/jenkins/view/Infinity/job/cassandra/job/1-build-linux/) job 
6. Use version from step 3 and stub universe URL from step 5 to trigger [release-to-universe](https://jenkins.mesosphere.com/service/jenkins/view/Infinity/job/infinity-tools/job/release-to-universe/) job.

# DSE

## Updating DataStax DSE binary package

We need to update the vanilla DataStax DSE binary package for following reasons:

1. Distribute DC/OS seed provider jar
2. Make JMX_PORT configurable
3. Remove default `cassandra-rackdc.properties` and `/cassandra-topology.properties`
4. Copy reporter-config JARs into lib directory for metric reporting:

To generate a new DataStax DSE binary package:

- **Option A:** Jenkins-based build/upload

0. Make any needed changes to [build-dse-bin.bash](build-dse-bin.bash), ex: version bumps.
1. Execute the [cassandra/build-upload-dist-dse](https://jenkins.mesosphere.com/service/jenkins/job/cassandra/job/build-upload-dist-dse/) job in Jenkins, with the latest commit sha from the current DSE release branch. Note that this will refuse to overwrite a file that already exists, but that shouldn't be a problem if a new version of DSE is being built, in which case the new filename would be different.
2. Update the universe's [resource.json](universe/resource.json) with new URL if necessary.

- **Option B:** Manual build/upload

Note: Because we currently maintain DSE as a separate branch, make sure that you are on the correct release branch before proceeding.

0. Make any needed changes to [build-dse-bin.bash](build-dse-bin.bash), ex: version bumps.
1. Execute the following:

  ```bash
  $ DSE_USERNAME=<datastax.com-password-here> DSE_PASSWORD=<datastax.com-password-here> ./build-dse-bin.bash
  ```
See [build-dse-bin.bash](build-dse-bin.bash) for more details on usage.
2. Upload the `/path/to/dcos-cassandra-service/cassandra-bin-tmp/dse-A.B.C-bin-dcos.tar.gz` to the designated release location.
3. Update the universe's [resource.json](universe/resource.json) with new URL if necessary.

## Releasing DC/OS DSE service

Note: Because we currently maintain DSE as a separate branch, make sure that you are on the correct release branch before proceeding.

1. Make necessary chanages to [dcos-cassandra-service/universe](universe)
2. Prepare RELEASE NOTES
3. Cut a tag with following format: `X.Y.Z-A.B.C`, where `X.Y.Z` is the version of service and `A.B.C` is the version of underlying DSE

  ```
  git tag X.Y.Z-A.B.C
  git push origin X.Y.Z-A.B.C
  ```

4. Trigger the [0-trigger-tag](https://jenkins.mesosphere.com/service/jenkins/view/Infinity/job/cassandra/job/0-trigger-tag/) job with the tag version created in step 3.
5. Once build job in step 4 finishes successfully, take the stub universe from the [1-build-linux](https://jenkins.mesosphere.com/service/jenkins/view/Infinity/job/cassandra/job/1-build-linux/) job 
6. Use version from step 3 and stub universe URL from step 5 to trigger [release-to-universe](https://jenkins.mesosphere.com/service/jenkins/view/Infinity/job/infinity-tools/job/release-to-universe/) job.
