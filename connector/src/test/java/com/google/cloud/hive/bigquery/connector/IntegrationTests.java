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
import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.hive.bigquery.connector.config.RunConf;
import com.google.cloud.hive.bigquery.connector.config.RunConf.Config;
import com.google.cloud.storage.*;
import com.klarna.hiverunner.*;
import com.klarna.hiverunner.builder.HiveShellBuilder;
import com.klarna.hiverunner.config.HiveRunnerConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.junit.jupiter.api.Test;
import repackaged.by.hivebqconnector.com.google.common.collect.ImmutableList;

// TODO: When running the tests, some noisy exceptions are displayed in the output:
//  "javax.jdo.JDOFatalUserException: Persistence Manager has been closed".
//  Those exceptions don't impact the execution of the tests, although they perhaps
//  make them run a bit slower overall. This seems related to:
//  https://issues.apache.org/jira/browse/HIVE-25261, which was fixed in Hive 4.0.0,
//  so we might have to find a workaround to make those go away with Hive 3.X.X.

public class IntegrationTests {

  private Path tmpDir;
  private HiveServerContainer hiveServerContainer; // Hive server
  private HiveShell hive; // Hive client

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
    initHive("mr", RunConf.AVRO);
  }

  public void initHive(String engine, String readDataFormat) {
    initHive(engine, readDataFormat, TEMP_GCS_PATH);
  }

  public void initHive(String engine, String readDataFormat, String tempGcsPath) {
    hive.setHiveConfValue(ConfVars.HIVE_EXECUTION_ENGINE.varname, engine);
    hive.setHiveConfValue(Config.READ_DATA_FORMAT.getKey(), readDataFormat);
    hive.setHiveConfValue(Config.TEMP_GCS_PATH.getKey(), tempGcsPath);
    hive.setHiveConfValue(
        "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"); // GCS Connector
    hive.start();
    hive.execute("CREATE DATABASE source_db");
  }

  // ---------------------------------------------------------------------------------------------------

  /**
   * Check that the user didn't forget to provide all the required properties when creating a Hive
   * table.
   */
  @Test
  public void testMissingTableProperties() {
    setUp();
    initHive();
    Throwable exception =
        assertThrows(
            RuntimeException.class,
            () ->
                hive.execute(
                    String.join(
                        "\n",
                        "CREATE TABLE some_table (number BIGINT, text" + " STRING)",
                        "STORED BY"
                            + " 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler';")));
    assertTrue(
        exception
            .getMessage()
            .contains(
                "The following table property(ies) must be provided: bq.project,"
                    + " bq.dataset, bq.table"));
    tearDown();
  }

  // ---------------------------------------------------------------------------------------------------

  /** Check that we tell the user when they use unsupported Hive types. */
  @Test
  public void testUnsupportedTypes() {
    setUp();
    initHive();
    for (String type : ImmutableList.of("float", "int", "smallint", "tinyint")) {
      Throwable exception =
          assertThrows(
              RuntimeException.class,
              () ->
                  hive.execute(
                      String.join(
                          "\n",
                          "CREATE TABLE " + TEST_TABLE_NAME + " (number " + type + ")",
                          "STORED BY"
                              + " 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'")));
      assertTrue(exception.getMessage().contains("Unsupported Hive type: " + type));
    }
    tearDown();
  }
}