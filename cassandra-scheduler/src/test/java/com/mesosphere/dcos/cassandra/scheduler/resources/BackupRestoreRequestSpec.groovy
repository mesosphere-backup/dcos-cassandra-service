package com.mesosphere.dcos.cassandra.scheduler.resources

import spock.lang.Specification

/**
 */
class BackupRestoreRequestSpec extends Specification {

  def "valid s3 backup request check"() {

    given:
    def request = new BackupRestoreRequest(name: name, externalLocation: externalLocation, s3AccessKey: s3AccessKey, s3SecretKey: s3SecretKey)

    expect:
    request.isValid() == valid

    where:
    name   | externalLocation | s3AccessKey | s3SecretKey || valid
    ""     | ""               | ""          | ""          || false
    ""     | "s3:"            | ""          | ""          || false
    null   | "s3:"            | ""          | ""          || false
    "name" | "s3:"            | "abc"       | null        || false
    "name" | "s3:"            | ""          | ""          || true
    "name" | "s3:"            | null        | null        || true
  }

  def "valid azure backup request check"() {

    given:
    def request = new BackupRestoreRequest(name: name, externalLocation: externalLocation, azureAccount: azureAccount, azureKey: azureKey)

    expect:
    request.isValid() == valid

    where:
    name   | externalLocation | azureAccount | azureKey || valid
    ""     | ""               | ""           | ""       || false
    ""     | "azure:"         | ""           | ""       || false
    null   | "azure:"         | ""           | ""       || false
    "name" | "azure:"         | ""           | ""       || true
  }

  def "invalid combinations of requests"() {

    when:
    def request = new BackupRestoreRequest(name: "name", externalLocation: "azure:", s3AccessKey: "", s3SecretKey: "")

    then:
    !request.isValid()

  }
}
