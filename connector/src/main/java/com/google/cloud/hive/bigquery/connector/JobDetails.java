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
import com.google.cloud.hive.bigquery.connector.utils.FileSystemUtils;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import repackaged.by.hivebqconnector.com.google.gson.Gson;

/**
 * Helper class that contains some information about the job. That information is persisted as a
 * JSON file on HDFS at the start of the job, so that we can look up that information at different
 * stages of the job.
 */
public class JobDetails {
  private String project;
  private String dataset;
  private String table;
  private boolean overwrite;
  private String finalTable; // Only used by the 'direct' write method
  private String gcsTempPath; // Only used by the 'indirect' write method
  private Properties tableProperties;
  private boolean isCTAS;

  public JobDetails() {}

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

  public boolean isOverwrite() {
    return overwrite;
  }

  public void setOverwrite(boolean overwrite) {
    this.overwrite = overwrite;
  }

  public TableId getFinalTableId() {
    if (finalTable == null) {
      return null;
    }
    return TableId.of(project, dataset, finalTable);
  }

  public void setFinalTable(String finalTable) {
    this.finalTable = finalTable;
  }

  public String getGcsTempPath() {
    return gcsTempPath;
  }

  public void setGcsTempPath(String gcsTempPath) {
    this.gcsTempPath = gcsTempPath;
  }

  public Properties getTableProperties() {
    return tableProperties;
  }

  public void setTableProperties(Properties tableProperties) {
    this.tableProperties = tableProperties;
  }

  public boolean isCTAS() {
    return isCTAS;
  }

  public void setCTAS(boolean CTAS) {
    isCTAS = CTAS;
  }

  /** Writes the job's details file to the job's work directory on HDFS. */
  public static void saveJobDetails(Configuration conf, JobDetails jobDetails) {
    Path path = FileSystemUtils.getJobDetailsFile(conf);
    FSDataOutputStream outputStream;
    try {
      outputStream = path.getFileSystem(conf).create(path);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    Gson gson = new Gson();
    try {
      outputStream.write(gson.toJson(jobDetails).getBytes(StandardCharsets.UTF_8));
      outputStream.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Reads the job's details file from the job's work directory on HDFS. */
  public static JobDetails getJobDetails(Configuration conf) {
    Path jobDetailsFile = FileSystemUtils.getJobDetailsFile(conf);
    try {
      if (FileSystem.get(conf).exists(jobDetailsFile)) {
        String jsonString = FileSystemUtils.readFile(conf, jobDetailsFile);
        Gson gson = new Gson();
        return gson.fromJson(jsonString, JobDetails.class);
      } else {
        return new JobDetails();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
