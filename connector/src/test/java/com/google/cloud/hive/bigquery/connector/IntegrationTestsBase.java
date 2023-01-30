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

import com.google.cloud.bigquery.*;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.storage.StorageException;
import com.klarna.hiverunner.HiveRunnerExtension;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Stream;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.provider.Arguments;

// TODO: When running the tests, some noisy exceptions are displayed in the output:
//  "javax.jdo.JDOFatalUserException: Persistence Manager has been closed".
//  Those exceptions don't impact the execution of the tests, although they perhaps
//  make them run a bit slower overall. This seems related to:
//  https://issues.apache.org/jira/browse/HIVE-25261, which was fixed in Hive 4.0.0,
//  so we might have to find a workaround to make those go away with Hive 3.X.X.

@ExtendWith(HiveRunnerExtension.class)
public class IntegrationTestsBase {

  protected static String dataset;

  @HiveSQL(
      files = {},
      autoStart = false)
  protected HiveShell hive;

  @BeforeAll
  public static void setUpAll() {
    // Create the bucket for 'indirect' jobs.
    try {
      createBucket(getIndirectWriteBucket());
    } catch (StorageException e) {
      if (e.getCode() == 409) {
        // The bucket already exists, maybe left over after a previous test failure.
        // Delete and recreate it to start fresh with an empty bucket.
        deleteBucket(getIndirectWriteBucket());
        createBucket(getIndirectWriteBucket());
      }
    }
    // Upload datasets to the BigLake bucket.
    uploadBlob(
        getBigLakeBucket(), "test.csv", "a,b,c\n1,2,3\n4,5,6".getBytes(StandardCharsets.UTF_8));
    // Create the test dataset in BigQuery
    dataset =
        String.format(
            "hive_bigquery_%d_%d",
            Long.MAX_VALUE - System.currentTimeMillis(), Long.MAX_VALUE - System.nanoTime());
    createBqDataset(dataset);
  }

  @BeforeEach
  public void setUpEach(TestInfo testInfo) {
    // Display which test is running
    String methodName = testInfo.getTestMethod().get().getName();
    String displayName = testInfo.getDisplayName();
    String parameters = "";
    if (!displayName.equals(methodName + "()")) {
      parameters = displayName;
    }
    System.out.printf(
        "\n---> Running test: %s.%s %s\n\n",
        testInfo.getTestClass().get().getName(),
        testInfo.getTestMethod().get().getName(),
        parameters);

    // Empty the indirect write bucket
    emptyBucket(getIndirectWriteBucket());
  }

  @AfterAll
  static void tearDownAll() {
    // Cleanup the GCS bucket
    deleteBucket(getIndirectWriteBucket());
    // Cleanup the test BQ dataset
    deleteBqDatasetAndTables(dataset);
  }

  public String renderQueryTemplate(String queryTemplate) {
    Map<String, Object> params = new HashMap<>();
    params.put("project", getProject());
    params.put("dataset", dataset);
    params.put("location", LOCATION);
    params.put("connection", BIGLAKE_CONNECTION);
    return StrSubstitutor.replace(queryTemplate, params, "${", "}");
  }

  public void createHiveTable(
      String tableName, String hiveDDL, boolean isExternal, String properties, String comment) {
    runHiveScript(
        String.join(
            "\n",
            "CREATE " + (isExternal ? "EXTERNAL" : "") + " TABLE " + tableName + " (",
            hiveDDL,
            ")",
            comment != null ? "COMMENT \"" + comment + "\"" : "",
            "STORED BY" + " 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'",
            "TBLPROPERTIES (",
            "  'bq.project'='${project}',",
            "  'bq.dataset'='${dataset}',",
            "  'bq.table'='" + tableName + "'",
            properties != null ? "," + properties : "",
            ");"));
  }

  public void createExternalTable(String tableName, String hiveDDL) {
    createHiveTable(tableName, hiveDDL, true, null, null);
  }

  public void createExternalTable(String tableName, String hiveDDL, String bqDDL) {
    createExternalTable(tableName, hiveDDL);
    createBqTable(tableName, bqDDL);
  }

  public void createManagedTable(String tableName, String hiveDDL) {
    createHiveTable(tableName, hiveDDL, false, null, null);
  }

  public void createManagedTable(
      String tableName, String hiveDDL, String properties, String comment) {
    createHiveTable(tableName, hiveDDL, false, properties, comment);
  }

  public void createBqTable(String tableName, String bqDDL) {
    createBqTable(tableName, bqDDL, null);
  }

  public void createBqTable(String tableName, String bqDDL, String description) {
    runBqQuery(
        String.join(
            "\n",
            "CREATE OR REPLACE TABLE ${dataset}." + tableName,
            "(",
            bqDDL,
            ")",
            description != null ? "OPTIONS ( description=\"" + description + "\" )" : ""));
  }

  public TableResult runBqQuery(String queryTemplate) {
    return getBigqueryClient().query(renderQueryTemplate(queryTemplate));
  }

  public void runHiveScript(String queryTemplate) {
    hive.execute(renderQueryTemplate(queryTemplate));
  }

  public List<Object[]> runHiveStatement(String queryTemplate) {
    return hive.executeStatement(renderQueryTemplate(queryTemplate));
  }

  public void initHive() {
    initHive("tez", HiveBigQueryConfig.ARROW);
  }

  public void initHive(String engine, String readDataFormat) {
    initHive(engine, readDataFormat, TEMP_GCS_PATH);
  }

  public void initHive(String engine, String readDataFormat, String tempGcsPath) {
    // Load potential Hive config values passed from system properties
    Map<String, String> hiveConfSystemOverrides = getHiveConfSystemOverrides();
    for (String key : hiveConfSystemOverrides.keySet()) {
      hive.setHiveConfValue(key, hiveConfSystemOverrides.get(key));
    }
    hive.setHiveConfValue(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname, engine);
    hive.setHiveConfValue(HiveBigQueryConfig.READ_DATA_FORMAT_KEY, readDataFormat);
    hive.setHiveConfValue(HiveBigQueryConfig.TEMP_GCS_PATH_KEY, tempGcsPath);
    hive.setHiveConfValue(
        "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"); // GCS Connector
    hive.setHiveConfValue("datanucleus.autoStartMechanismMode", "ignored");
    hive.start();
    runHiveScript("CREATE DATABASE source_db");
  }

  protected static final String EXECUTION_ENGINE = "executionEngineParameter";

  protected static Stream<Arguments> executionEngineParameter() {
    List<String> engines = Arrays.asList("mr", "tez");
    return Stream.of(Arguments.of(engines.get(0)), Arguments.of(engines.get(1)));
  }

  protected static final String READ_FORMAT = "readFormatParameter";

  protected static Stream<Arguments> readFormatParameter() {
    List<String> readFormats = Arrays.asList(HiveBigQueryConfig.ARROW, HiveBigQueryConfig.AVRO);
    return Stream.of(Arguments.of(readFormats.get(0)), Arguments.of(readFormats.get(1)));
  }

  protected static final String EXECUTION_ENGINE_READ_FORMAT =
      "executionEngineReadFormatParameters";

  protected static Stream<Arguments> executionEngineReadFormatParameters() {
    List<String> engines = Arrays.asList("mr", "tez");
    List<String> readFormats = Arrays.asList(HiveBigQueryConfig.ARROW, HiveBigQueryConfig.AVRO);
    Collections.shuffle(readFormats);
    return Stream.of(
        Arguments.of(engines.get(0), readFormats.get(0)),
        Arguments.of(engines.get(1), readFormats.get(1)));
  }

  protected static final String EXECUTION_ENGINE_WRITE_METHOD =
      "executionEngineWriteMethodParameters";

  protected static Stream<Arguments> executionEngineWriteMethodParameters() {
    List<String> engines = Arrays.asList("mr", "tez");
    List<String> writeMethods =
        Arrays.asList(
            HiveBigQueryConfig.WRITE_METHOD_DIRECT, HiveBigQueryConfig.WRITE_METHOD_INDIRECT);
    Collections.shuffle(writeMethods);
    return Stream.of(
        Arguments.of(engines.get(0), writeMethods.get(0)),
        Arguments.of(engines.get(1), writeMethods.get(1)));
  }

  protected static final String EXECUTION_ENGINE_READ_FORMAT_WRITE_METHOD =
      "executionEngineReadFormatWriteMethodParameters";

  protected static Stream<Arguments> executionEngineReadFormatWriteMethodParameters() {
    List<String> engines = Arrays.asList("mr", "tez");
    List<String> readFormats = Arrays.asList(HiveBigQueryConfig.ARROW, HiveBigQueryConfig.AVRO);
    List<String> writeMethods =
        Arrays.asList(
            HiveBigQueryConfig.WRITE_METHOD_DIRECT, HiveBigQueryConfig.WRITE_METHOD_INDIRECT);
    Collections.shuffle(readFormats);
    Collections.shuffle(writeMethods);
    return Stream.of(
        Arguments.of(engines.get(0), readFormats.get(0), writeMethods.get(0)),
        Arguments.of(engines.get(1), readFormats.get(1), writeMethods.get(1)));
  }
}
