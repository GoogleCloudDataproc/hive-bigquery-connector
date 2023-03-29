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

import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.hive.bigquery.connector.utils.FileSystemUtils;
import com.google.cloud.hive.bigquery.connector.utils.bq.BigQueryUtils;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import shaded.hivebqcon.com.google.gson.*;

/**
 * Helper class that contains some information about the job. That information is persisted as a
 * JSON file on HDFS at the start of the job, so that we can look up that information at different
 * stages of the job.
 */
public class JobDetails {
  // private String project;
  // private String dataset;
  // private String table;
  private TableId tableId;
  private TableId finalTableId;
  private boolean overwrite;
  private String finalTable; // Only used by the 'direct' write method
  private String gcsTempPath; // Only used by the 'indirect' write method
  private Properties hmsTableProperties;
  private transient Schema bigquerySchema;
  private String bigquerySchemaJSON;
  private transient org.apache.avro.Schema avroSchema; // Only used by the 'indirect' write method
  private String avroSchemaJSON; // Only used by the 'indirect' write method

  public JobDetails() {}

  public void setTableId(TableId tableId) {
    this.tableId = tableId;
  }

  public TableId getTableId() {
    return this.tableId;
  }

  public void setFinalTableId(TableId finalTableId) {
    this.finalTableId = finalTableId;
  }

  public TableId getFinalTableId() {
    return this.finalTableId;
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

  public String getHmsDbTableName() {
    return this.hmsTableProperties.getProperty("name");
  }

  public String getJobGcsTempPath() {
    return gcsTempPath + "/" + getHmsDbTableName();
  }

  public Properties getTableProperties() {
    return hmsTableProperties;
  }

  public void setTableProperties(Properties hmsTableProperties) {
    this.hmsTableProperties = hmsTableProperties;
  }

  public Schema getBigquerySchema() {
    if (bigquerySchema == null && bigquerySchemaJSON != null) {
      bigquerySchema = BigQueryUtils.loadSchemaFromJSON(bigquerySchemaJSON);
    }
    return bigquerySchema;
  }

  public void setBigquerySchema(Schema schema) {
    if (schema != null) {
      bigquerySchemaJSON = BigQueryUtils.exportSchemaToJSON(schema);
    }
    this.bigquerySchema = schema;
  }

  public org.apache.avro.Schema getAvroSchema() {
    if (avroSchema == null && avroSchemaJSON != null) {
      avroSchema = org.apache.avro.Schema.parse(avroSchemaJSON);
    }
    return avroSchema;
  }

  public void setAvroSchema(org.apache.avro.Schema schema) {
    if (schema != null) {
      avroSchemaJSON = schema.toString();
    }
    this.avroSchema = schema;
  }

  /** Reads the job's details file from the job's work directory on HDFS. */
  public static JobDetails readJobDetailsFile(Configuration conf, String hmsDbTableName)
      throws IOException {
    Path jobDetailsFilePath = FileSystemUtils.getJobDetailsFilePath(conf, hmsDbTableName);
    String jsonString = FileSystemUtils.readFile(conf, jobDetailsFilePath);
    Gson gson = new Gson();
    return gson.fromJson(jsonString, JobDetails.class);
  }

  /** Reads the job's details file from the job's work path. */
  public static JobDetails readJobDetailsFile(Configuration conf, Path path) throws IOException {
    String jsonString = FileSystemUtils.readFile(conf, path);
    Gson gson = new Gson();
    return gson.fromJson(jsonString, JobDetails.class);
  }

  /** Writes the job's details file to the job's work directory in the path. */
  public static void writeJobDetailsFile(Configuration conf, Path path, JobDetails jobDetails) {
    try {
      FSDataOutputStream infoFile = path.getFileSystem(conf).create(path);
      Gson gson = new Gson();
      infoFile.write(gson.toJson(jobDetails).getBytes(StandardCharsets.UTF_8));
      infoFile.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
