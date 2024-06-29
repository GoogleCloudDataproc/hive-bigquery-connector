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
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.utils.FileSystemUtils;
import com.google.cloud.hive.bigquery.connector.utils.JobUtils;
import com.google.cloud.hive.bigquery.connector.utils.bq.BigQueryUtils;
import com.google.gson.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class that contains some information about the job. That information is persisted as a
 * JSON file on HDFS at the start of the job, so that we can look up that information at different
 * stages of the job.
 */
public class JobDetails {
  private static final Logger LOG = LoggerFactory.getLogger(JobDetails.class);

  private TableId tableId;
  private TableId finalTableId;
  private boolean overwrite;
  private Properties hmsTableProperties;
  private transient Schema bigquerySchema;
  private String bigquerySchemaJSON;
  private transient org.apache.avro.Schema avroSchema; // Only used by the 'indirect' write method
  private String avroSchemaJSON; // Only used by the 'indirect' write method
  private String writeMethod;
  private boolean deleteTableOnAbort;

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

  public String getHmsDbTableName() {
    return this.hmsTableProperties.getProperty("name");
  }

  public Properties getTableProperties() {
    return hmsTableProperties;
  }

  public void setTableProperties(Properties hmsTableProperties) {
    this.hmsTableProperties = hmsTableProperties;
  }

  public String getWriteMethod() {
    return writeMethod;
  }

  public void setWriteMethod(String writeMethod) {
    this.writeMethod = writeMethod;
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

  /** Reads the job's details file from the job's work directory on disk */
  public static JobDetails readJobDetailsFile(Configuration conf, String hmsDbTableName)
      throws IOException {
    Path jobDetailsFilePath = JobUtils.getJobDetailsFilePath(conf, hmsDbTableName);
    return readJobDetailsFile(conf, jobDetailsFilePath);
  }

  /**
   * Clean up everything related to this job details: the job details file, its parent directory,
   * and the GCS output Avro files for the indirect writes.
   */
  public void cleanUp(Configuration conf) throws IOException {
    try {
      LOG.debug("Start cleaning up Job Details: {}", getFilePath(conf));
      JobUtils.deleteJobDetailsDir(conf, this);
      // Delete the query work dir if it's now empty
      FileSystemUtils.deleteIfEmpty(conf, JobUtils.getQueryWorkDir(conf));
      if (writeMethod.equals(HiveBigQueryConfig.WRITE_METHOD_INDIRECT)) {
        Path tempOutputPath =
            JobUtils.getQueryTempOutputPath(conf, getTableProperties(), getHmsDbTableName());
        // Delete Avro files from GCS
        LOG.info("Deleting job temporary output directory {}", conf);
        FileSystemUtils.deleteDir(conf, tempOutputPath);
        // Delete the temp output dir's parent directory if it's now empty
        FileSystemUtils.deleteIfEmpty(conf, tempOutputPath.getParent());
      }
      LOG.debug("Finished cleaning up Job Details: {}", getFilePath(conf));
    } catch (Exception e) {
      LOG.warn("Failed cleaning up Job Details: {}. Error: {}", getFilePath(conf), e);
    }
  }

  /** Returns the path of the job details file on disk */
  public Path getFilePath(Configuration conf) {
    String hmsTableName = (String) getTableProperties().get("name");
    assert hmsTableName != null;
    return JobUtils.getJobDetailsFilePath(conf, hmsTableName);
  }

  /** Reads the job's details file from the job's work path. */
  public static JobDetails readJobDetailsFile(Configuration conf, Path path) throws IOException {
    String jsonString = FileSystemUtils.readFile(conf, path);
    Gson gson = new Gson();
    return gson.fromJson(jsonString, JobDetails.class);
  }

  /** Writes the job's details file on disk */
  public void writeFile(Configuration conf) {
    try {
      Path path = getFilePath(conf);
      FSDataOutputStream infoFile = path.getFileSystem(conf).create(path);
      Gson gson = new Gson();
      infoFile.write(gson.toJson(this).getBytes(StandardCharsets.UTF_8));
      infoFile.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean isDeleteTableOnAbort() {
    return deleteTableOnAbort;
  }

  public void setDeleteTableOnAbort(boolean deleteTableOnAbort) {
    this.deleteTableOnAbort = deleteTableOnAbort;
  }
}
