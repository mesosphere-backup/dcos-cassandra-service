/*
 * Copyright 2016 Mesosphere
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mesosphere.dcos.cassandra.common.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mesosphere.dcos.cassandra.common.CassandraProtos;
import com.mesosphere.dcos.cassandra.common.util.JsonUtils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;
import java.util.*;

/**
 * HeapConfig is contains the configuration for the JVM heap for a Cassandra
 * node. The size of the heap is the size in Mb and should be 1/4 of the
 * allocated system memory and no greater than 8 Mb.  The new size is the size
 * of the new generation in Mb. It should be set at 100 Mb per cpu core.
 */
public class HeapConfig {

  public enum GC_TYPE {
    G1,
    CMS
  }

  public static final String DEFAULT_FILE_NAME = "jvm.options";

  private static final List<String> CMS_SETTINGS =
    Arrays.asList(
      "-XX:+UseParNewGC",
      "-XX:+UseConcMarkSweepGC",
      "-XX:+CMSParallelRemarkEnabled",
      "-XX:SurvivorRatio=8",
      "-XX:MaxTenuringThreshold=1",
      "-XX:CMSInitiatingOccupancyFraction=75",
      "-XX:+UseCMSInitiatingOccupancyOnly",
      "-XX:CMSWaitDuration=10000",
      "-XX:+CMSParallelInitialMarkEnabled",
      "-XX:+CMSEdenChunksRecordAlways",
      "-XX:+CMSClassUnloadingEnabled");

  private static final List<String> G1_SETTINGS =
    Arrays.asList(
      "-XX:+UseG1GC",
      "-XX:G1RSetUpdatingPauseTimePercent=5",
      "-XX:MaxGCPauseMillis=500");

  /**
   * The default heap configuration for a Cassandra node.
   */
  public static final HeapConfig DEFAULT =
    HeapConfig.create(2048, 100, GC_TYPE.CMS);

  @JsonProperty("size_mb")
  private final int sizeMb;
  @JsonProperty("new_mb")
  private final int newMb;
  @JsonProperty("gc_type")
  private final GC_TYPE gcType;

  /**
   * Factory method creates a new HeapConfig.
   *
   * @param sizeMb The size of the JVM heap in Mb.
   * @param newMb  The size of the new generation in Mb.
   * @return A HeapConfig constructed from the parameters.
   */
  @JsonCreator
  public static HeapConfig create(
    @JsonProperty("size_mb") final int sizeMb,
    @JsonProperty("new_mb") final int newMb,
    @JsonProperty("gc_type") final GC_TYPE gcType) {
    return new HeapConfig(sizeMb, newMb, gcType);
  }

  /**
   * Parses a HeapConfig from a Protocol Buffers format.
   *
   * @param heap The Protocol Buffers format of a HeapConfig.
   * @return The HeapConfig parsed from heap.
   */
  public static HeapConfig parse(CassandraProtos.HeapConfig heap) {
    return create(
      heap.getSizeMb(),
      heap.getNewMb(),
      GC_TYPE.values()[heap.getGcType()]);
  }

  /**
   * Parses a HeapConfig from a byte array containing a Protocol Buffers
   * serialized format.
   *
   * @param bytes A byte array containing a Protocol Buffers serialized
   *              format of a HeapConfig.
   * @return A HeapConfig parsed from bytes.
   * @throws IOException If a HeapConfig can not be parsed from bytes.
   */
  public static HeapConfig parse(byte[] bytes) throws IOException {

    return parse(CassandraProtos.HeapConfig.parseFrom(bytes));
  }

  /**
   * Constructs a new HeapConfig
   *
   * @param sizeMb The size of the JVM heap in Mb.
   * @param newMb  The size of the new generation in Mb.
   */
  public HeapConfig(final int sizeMb, final int newMb, final GC_TYPE gcType) {
    this.newMb = newMb;
    this.sizeMb = sizeMb;
    this.gcType = gcType;
  }

  /**
   * Gets the heap size.
   *
   * @return The size of the JVM heap in Mb.
   */
  @JsonIgnore
  public int getSizeMb() {
    return sizeMb;
  }

  /**
   * Gets the new generation size.
   *
   * @return The size of the new generation in Mb.
   */
  @JsonIgnore
  public int getNewMb() {
    return newMb;
  }

  /**
   * Gets the Garbage Collector type.
   *
   * @return The Garbage Collector type.
   */
  @JsonIgnore
  public GC_TYPE getGcType() {
    return gcType;
  }

  /**
   * Gets a Protocol Buffers representation of the HeapConfig.
   *
   * @return A Protocol Buffers serialized representation of the HeapConfig.
   */
  public CassandraProtos.HeapConfig toProto() {
    return CassandraProtos.HeapConfig.newBuilder()
      .setSizeMb(sizeMb)
      .setNewMb(newMb)
      .setGcType(gcType.ordinal())
      .build();
  }

  /**
   * Gets a byte array containing the Protocol Buffers serialized
   * representation of the HeapConfig.
   *
   * @return A byte array containing the Protocol Buffers serialized
   * representation of the HeapConfig.
   */
  public byte[] toByteArray() {

    return toProto().toByteArray();
  }

  @JsonIgnore
  protected String getXmx() {
    return "-Xmx" + sizeMb + "M";
  }

  @JsonIgnore
  protected String getXms() {
    return "-Xms" + sizeMb + "M";
  }

  @JsonIgnore
  protected String getXmn() {
    return "-Xmn" + newMb + "M";
  }

  @JsonIgnore
  public List<String> getHeapSettings() {
    List<String> config = Arrays.asList(getXms(), getXmx());
    if (gcType.equals(GC_TYPE.CMS)) {
      config.add(getXmn());
      config.addAll(CMS_SETTINGS);
    } else {
      config.addAll(G1_SETTINGS);
    }
    return config;
  }

  public void writeHeapSettings(final Path path) throws IOException {
    FileWriter writer = null;
    try {
      writer = new FileWriter(path.toFile(), false);
      List<String> settings = getHeapSettings();
      for(String setting:settings){
        writer.write(setting + '\n');
      }
      writer.flush();
    } finally {
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException ex) {
        }
      }
    }
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    HeapConfig that = (HeapConfig) o;

    if (sizeMb != that.sizeMb) return false;
    if (newMb != that.newMb) return false;
    return gcType == that.gcType;

  }

  @Override
  public int hashCode() {
    int result = sizeMb;
    result = 31 * result + newMb;
    result = 31 * result + (gcType != null ? gcType.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return JsonUtils.toJsonString(this);
  }
}
