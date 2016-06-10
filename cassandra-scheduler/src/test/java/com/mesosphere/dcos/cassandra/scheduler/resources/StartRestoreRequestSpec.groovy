package com.mesosphere.dcos.cassandra.scheduler.resources

import spock.lang.Specification

/**
 *
 */
class StartRestoreRequestSpec extends Specification {
  def "valid s3 backup request check"() {

    given:
    def request = new StartRestoreRequest(name: name, externalLocation: externalLocation, s3AccessKey: s3AccessKey, s3SecretKey: s3SecretKey)

    expect:
    request.isValid() == valid

    where:
    name   | externalLocation | s3AccessKey | s3SecretKey || valid
    ""     | ""               | ""          | ""          || false
    ""     | "s3:"            | ""          | ""          || false
    null   | "s3:"            | ""          | ""          || false
    "name" | "s3:"            | ""          | ""          || true
  }

  def "valid azure backup request check"() {

    given:
    def request = new StartRestoreRequest(name: name, externalLocation: externalLocation, azureAccount: azureAccount, azureKey: azureKey)

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
    def request = new StartRestoreRequest(name: "name", externalLocation: "azure:", s3AccessKey: "", s3SecretKey: "")

    then:
    !request.isValid()

    when:
    request = new StartRestoreRequest(name: "name", externalLocation: "s3:", azureAccount: "", azureKey: "")

    then:
    !request.isValid()
  }
}
