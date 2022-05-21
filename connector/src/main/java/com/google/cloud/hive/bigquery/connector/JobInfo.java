/*
 * Copyright 2022 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.hive.bigquery.connector;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.hive.bigquery.connector.utils.FSUtils;
import com.google.cloud.hive.bigquery.connector.utils.avro.AvroUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import repackaged.by.hivebqconnector.com.google.gson.Gson;
import repackaged.by.hivebqconnector.com.google.protobuf.InvalidProtocolBufferException;

/**
 * Helper class that contains some information about the job. That information is persisted as a
 * JSON file on HDFS at the start of the job, so that we can look up that information at different
 * stages of the job.
 */
public class JobInfo {
  private String project;
  private String dataset;
  private String table;
  private String finalTable; // Only used by the 'direct' write method
  private boolean overwrite;
  private String gcsTempPath; // Only used by the 'file-load' write method
  private String avroSchema; // Only used by the 'file-load' write method
  private byte[] protoSchema; // Only used by the 'direct' write method
  private byte[] bigQuerySchema; // Only used by the 'direct' write method
  private Properties tableProperties;

  public JobInfo() {}

  public String getProject() {
    return project;
  }

  public void setProject(String project) {
    this.project = project;
  }

  public String getDataset() {
    return dataset;
  }

  public void setDataset(String dataset) {
    this.dataset = dataset;
  }

  public TableId getTableId() {
    return TableId.of(project, dataset, table);
  }

  public void setTable(String table) {
    this.table = table;
  }

  public TableId getFinalTableId() {
    return TableId.of(project, dataset, finalTable);
  }

  public void setFinalTable(String finalTable) {
    this.finalTable = finalTable;
  }

  public boolean isOverwrite() {
    return overwrite;
  }

  public void setOverwrite(boolean overwrite) {
    this.overwrite = overwrite;
  }

  public String getGcsTempPath() {
    return gcsTempPath;
  }

  public void setGcsTempPath(String gcsTempPath) {
    this.gcsTempPath = gcsTempPath;
  }

  public Schema getAvroSchema() {
    return (avroSchema == null ? null : AvroUtils.parseSchema(avroSchema));
  }

  public void setAvroSchema(String avroSchema) {
    this.avroSchema = avroSchema;
  }

  public ProtoSchema getProtoSchema() {
    try {
      return (protoSchema == null ? null : ProtoSchema.parseFrom(protoSchema));
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public void setProtoSchema(byte[] protoSchema) {
    this.protoSchema = protoSchema;
  }

  public com.google.cloud.bigquery.Schema getBigQuerySchema() {
    ByteArrayInputStream bais = new ByteArrayInputStream(bigQuerySchema);
    com.google.cloud.bigquery.Schema schema;
    try {
      ObjectInputStream ois = new ObjectInputStream(bais);
      schema = (com.google.cloud.bigquery.Schema) ois.readObject();
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    return schema;
  }

  public void setBigQuerySchema(byte[] bigQuerySchema) {
    this.bigQuerySchema = bigQuerySchema;
  }

  public Properties getTableProperties() {
    return tableProperties;
  }

  public void setTableProperties(Properties tableProperties) {
    this.tableProperties = tableProperties;
  }

  /** Writes the job's info file to the job's work directory on HDFS. */
  public static void writeInfoFile(Configuration conf, JobInfo jobInfo) {
    Path path = FSUtils.getInfoFile(conf);
    FSDataOutputStream infoFile;
    try {
      infoFile = path.getFileSystem(conf).create(path);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    Gson gson = new Gson();
    try {
      infoFile.write(gson.toJson(jobInfo).getBytes(StandardCharsets.UTF_8));
      infoFile.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Reads the job's info file from the job's work directory on HDFS. */
  public static JobInfo readInfoFile(Configuration conf) throws IOException {
    String jsonString = FSUtils.readFile(conf, FSUtils.getInfoFile(conf));
    Gson gson = new Gson();
    return gson.fromJson(jsonString, JobInfo.class);
  }
}
