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

import static com.google.cloud.hive.bigquery.connector.TestUtils.*;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.storage.StorageException;
import com.klarna.hiverunner.HiveServerContainer;
import com.klarna.hiverunner.HiveServerContext;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveServerContext;
import com.klarna.hiverunner.builder.HiveShellBuilder;
import com.klarna.hiverunner.config.HiveRunnerConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

public class IntegrationTestsBase {

  protected Path tmpDir;
  protected HiveServerContainer hiveServerContainer; // Hive server
  protected HiveShell hive; // Hive client

  public void setUp() {
    // Set up the Hive server and client
    try {
      tmpDir = Files.createTempDirectory("hiverunner_test");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    HiveServerContext context = new StandaloneHiveServerContext(tmpDir, new HiveRunnerConfig());
    hiveServerContainer = new HiveServerContainer(context);
    HiveShellBuilder hiveBuilder = new HiveShellBuilder();
    hiveBuilder.setHiveServerContainer(hiveServerContainer);
    hive = hiveBuilder.buildShell();
    // Create the test dataset and table in BigQuery
    try {
      createDataset(DATASET);
    } catch (BigQueryException e) {
      if (e.getMessage().contains("Already Exists")) {
        deleteDatasetAndTables(DATASET);
        createDataset(DATASET);
      }
    }
    // Create the bucket for 'indirect' jobs.
    try {
      createBucket(TEMP_BUCKET_NAME);
    } catch (StorageException e) {
      if (e.getCode() == 409) { // Bucket already exists
        deleteBucket(TEMP_BUCKET_NAME);
        createBucket(TEMP_BUCKET_NAME);
      }
    }
  }

  public void tearDown() {
    // Cleanup the test BQ dataset and GCS bucket
    deleteDatasetAndTables(DATASET);
    deleteBucket(TEMP_BUCKET_NAME);
    // Tear down the Hive server
    hiveServerContainer.tearDown();
    try {
      FileUtils.deleteDirectory(tmpDir.toFile());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void initHive() {
    initHive("mr", HiveBigQueryConfig.AVRO);
  }

  public void initHive(String engine, String readDataFormat) {
    initHive(engine, readDataFormat, TEMP_GCS_PATH);
  }

  public void initHive(String engine, String readDataFormat, String tempGcsPath) {
    hive.setHiveConfValue(ConfVars.HIVE_EXECUTION_ENGINE.varname, engine);
    hive.setHiveConfValue(HiveBigQueryConfig.READ_DATA_FORMAT_KEY, readDataFormat);
    hive.setHiveConfValue(HiveBigQueryConfig.TEMP_GCS_PATH_KEY, tempGcsPath);
    hive.setHiveConfValue(
        "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"); // GCS Connector
    hive.start();
    hive.execute("CREATE DATABASE source_db");
  }
}
