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
package com.google.cloud.hive.bigquery.connector.integration;

import static com.google.cloud.hive.bigquery.connector.TestUtils.*;
import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.bigquery.*;
import com.google.cloud.hive.bigquery.connector.TestLogAppender;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.utils.JobUtils;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.StorageException;
import com.klarna.hiverunner.HiveRunnerExtension;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
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
public abstract class IntegrationTestsBase {

  protected static String dataset;

  // Stores the current system properties before each test so we can restore them afterwards
  private Properties systemPropertiesBackup;

  // Temp bucket for indirect writes.
  protected static String testBucketName;

  // Relative path under the temp test bucket for indirect writes.
  protected static final String NON_EXISTING_PATH = "gs://random-x8e0dAfe";
  protected static final String TEMP_GCS_DIR_PREFIX = "temp-integration-test/";
  protected String tempGcsDir = TEMP_GCS_DIR_PREFIX;

  // Logging
  Level originalLogLevel;
  protected TestLogAppender testLogAppender = new TestLogAppender();

  @HiveSQL(
      files = {},
      autoStart = false)
  protected HiveShell hive;

  @BeforeAll
  public static void setUpAll() {
    testBucketName = getTestBucket();

    // Create the temp bucket for indirect writes if it does not exist.
    try {
      createBucket(testBucketName);
    } catch (StorageException e) {
      if (e.getCode() == 409) {
        // The bucket already exists, which is okay.
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
    originalLogLevel = Logger.getRootLogger().getLevel();
    // Save the current state of system properties before each test
    systemPropertiesBackup = new Properties();
    Enumeration<?> propertyNames = System.getProperties().propertyNames();
    while (propertyNames.hasMoreElements()) {
      String key = propertyNames.nextElement().toString();
      String value = System.getProperty(key);
      if (value != null) {
        systemPropertiesBackup.put(key, value);
      }
    }

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

    // Set default Hadoop/Hive configuration -----------------------------------
    // TODO: Match with Dataproc's default config as much as possible
    // Enable map-joins
    System.getProperties().setProperty(HiveConf.ConfVars.HIVECONVERTJOIN.varname, "true");
    // Enable vectorize mode
    System.getProperties()
        .setProperty(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED.varname, "true");
    String timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
    this.tempGcsDir = TEMP_GCS_DIR_PREFIX + timestamp;
  }

  @AfterEach
  public void tearDownEach(TestInfo testInfo) {
    // Restore logging settings
    Logger rootLogger = Logger.getRootLogger();
    rootLogger.setLevel(originalLogLevel);
    rootLogger.removeAppender(testLogAppender);
    testLogAppender.clear();
    // Clean up GCS
    if (tempGcsDir != null && tempGcsDir.startsWith(TEMP_GCS_DIR_PREFIX)) {
      emptyGcsDir(testBucketName, tempGcsDir);
      tempGcsDir = null;
    }

    // Restore the original system properties after each test
    System.setProperties(systemPropertiesBackup);
  }

  @AfterAll
  static void tearDownAll() {
    // Cleanup the test BQ dataset
    deleteBqDatasetAndTables(dataset);
  }

  public String renderQueryTemplate(String queryTemplate) {
    Map<String, Object> params = new HashMap<>();
    params.put("project", getProject());
    params.put("dataset", dataset);
    params.put("location", LOCATION);
    params.put("connection", getBigLakeConnection());
    params.put("test_bucket", "gs://" + testBucketName);
    return StrSubstitutor.replace(queryTemplate, params, "${", "}");
  }

  public void initLoggingCapture(Level level) {
    Logger rootLogger = Logger.getRootLogger();
    rootLogger.setLevel(level);
    rootLogger.addAppender(testLogAppender);
  }

  public void clearLogs() {
    testLogAppender.clear();
  }

  public void assertLogsContain(String s) {
    assertTrue(
        testLogAppender.getLogs().stream()
            .anyMatch(event -> event.getRenderedMessage().contains(s)));
  }

  public void createPureHiveTable(String tableName, String hiveDDL) {
    runHiveQuery(String.join("\n", "CREATE TABLE " + tableName + " (", hiveDDL, ")"));
  }

  public void createHiveTable(
      String tableName, String hiveDDL, boolean isExternal, String properties, String comment) {
    runHiveQuery(
        String.join(
            "\n",
            "CREATE " + (isExternal ? "EXTERNAL" : "") + " TABLE " + tableName + " (",
            hiveDDL,
            ")",
            comment != null ? "COMMENT \"" + comment + "\"" : "",
            "STORED BY" + " 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'",
            "TBLPROPERTIES (",
            "  'bq.table'='${project}.${dataset}." + tableName + "'",
            properties != null ? "," + properties : "",
            ")"));
  }

  public void createHiveTable(
      String hmsTableName,
      String bqTableName,
      String hiveDDL,
      boolean isExternal,
      String properties,
      String comment) {
    runHiveQuery(
        String.join(
            "\n",
            "CREATE " + (isExternal ? "EXTERNAL" : "") + " TABLE " + hmsTableName + " (",
            hiveDDL,
            ")",
            comment != null ? "COMMENT \"" + comment + "\"" : "",
            "STORED BY" + " 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'",
            "TBLPROPERTIES (",
            "  'bq.table'='${project}.${dataset}." + bqTableName + "'",
            properties != null ? "," + properties : "",
            ")"));
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

  /** Runs a Hive script, which may be made of multiple Hive statements. */
  public void runHiveScript(String queryTemplate) {
    hive.execute(renderQueryTemplate(queryTemplate));
  }

  /** Runs a single Hive statement. */
  public List<Object[]> runHiveQuery(String queryTemplate) {
    // Remove the ';' character at the end if there is one
    String cleanedTemplate = StringUtils.stripEnd(queryTemplate, null);
    if (StringUtils.endsWith(queryTemplate, ";")) {
      cleanedTemplate = StringUtils.chop(cleanedTemplate);
    }
    return hive.executeStatement(renderQueryTemplate(cleanedTemplate));
  }

  public void initHive() {
    initHive(getDefaultExecutionEngine());
  }

  public void initHive(String engine) {
    initHive(engine, HiveBigQueryConfig.ARROW);
  }

  public void initHive(String engine, String readDataFormat) {
    String tempGcsPath = "gs://" + testBucketName + "/" + tempGcsDir;
    initHive(engine, readDataFormat, tempGcsPath);
  }

  public void initHive(String engine, String readDataFormat, String tempGcsPath) {
    tempGcsDir = tempGcsPath.replaceFirst("gs://" + testBucketName + "/", "");
    assertTrue(
        tempGcsDir.startsWith(TEMP_GCS_DIR_PREFIX)
            || tempGcsPath.equals(NON_EXISTING_PATH)
            || tempGcsPath.isEmpty());

    // Load potential Hive config values passed from system properties
    Map<String, String> hiveConfSystemOverrides = getHiveConfSystemOverrides();
    for (String key : hiveConfSystemOverrides.keySet()) {
      System.getProperties().setProperty(key, hiveConfSystemOverrides.get(key));
    }
    System.getProperties().setProperty(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname, engine);
    System.getProperties().setProperty(HiveBigQueryConfig.READ_DATA_FORMAT_KEY, readDataFormat);
    System.getProperties().setProperty(HiveBigQueryConfig.TEMP_GCS_PATH_KEY, tempGcsPath);
    System.getProperties()
        .setProperty(
            "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"); // GCS Connector
    System.getProperties().setProperty("datanucleus.autoStartMechanismMode", "ignored");

    // This is needed to avoid an odd exception when running the tests with Tez and Hadoop 3.
    // Similar issue to what's described in https://issues.apache.org/jira/browse/HIVE-24734
    System.getProperties().setProperty(MRJobConfig.MAP_MEMORY_MB, "1024");

    // Apply system properties to the Hive conf
    Enumeration<?> propertyNames = System.getProperties().propertyNames();
    while (propertyNames.hasMoreElements()) {
      String key = propertyNames.nextElement().toString();
      String value = System.getProperty(key);
      if (value != null) {
        hive.setHiveConfValue(key, value);
      }
    }

    hive.start();
    runHiveQuery("CREATE DATABASE source_db");
  }

  public void checkThatWorkDirsHaveBeenCleaned() {
    File[] files = (new File(hive.getHiveConf().get("hadoop.tmp.dir"))).listFiles();
    // Only tolerate the presence of a "mapred" folder, where MapReduce stores its
    // own work files
    if (files.length > 0) {
      assertEquals(1, files.length);
      assertEquals("mapred", files[0].getName());
    }
    // Check that the Avro files have been cleaned up for the indirect write method
    if (hive.getHiveConf()
        .get(HiveBigQueryConfig.WRITE_METHOD_KEY)
        .equals(HiveBigQueryConfig.WRITE_METHOD_INDIRECT)) {
      String tmpGcsPath = hive.getHiveConf().get(HiveBigQueryConfig.TEMP_GCS_PATH_KEY);
      String bucketName = JobUtils.extractBucketNameFromGcsUri(tmpGcsPath);
      String dirName = tmpGcsPath.replaceFirst("gs://" + bucketName + "/", "");
      List<Blob> blobs = getBlobs(bucketName, dirName);
      // Check that all that's left is the temp output directory itself
      assertEquals(1, blobs.size());
      assertEquals(dirName + "/", blobs.get(0).getName());
    }
  }

  protected static String getDefaultExecutionEngine() {
    return "tez";
  }

  protected static final String EXECUTION_ENGINE = "executionEngineParameter";

  protected static Stream<Arguments> executionEngineParameter() {
    return Stream.of(Arguments.of("mr"), Arguments.of("tez"));
  }

  protected static final String READ_FORMAT = "readFormatParameter";

  protected static Stream<Arguments> readFormatParameter() {
    return Stream.of(Arguments.of(HiveBigQueryConfig.ARROW), Arguments.of(HiveBigQueryConfig.AVRO));
  }

  protected static final String WRITE_METHOD = "writeMethodParameters";

  protected static Stream<Arguments> writeMethodParameters() {
    return Stream.of(
        Arguments.of(HiveBigQueryConfig.WRITE_METHOD_DIRECT),
        Arguments.of(HiveBigQueryConfig.WRITE_METHOD_INDIRECT));
  }

  protected static final String EXECUTION_ENGINE_READ_FORMAT =
      "executionEngineReadFormatParameters";

  protected static Stream<Arguments> executionEngineReadFormatParameters() {
    List<String> readFormats = Arrays.asList(HiveBigQueryConfig.ARROW, HiveBigQueryConfig.AVRO);
    Collections.shuffle(readFormats);
    return Stream.of(
        Arguments.of("mr", readFormats.get(0)), Arguments.of("tez", readFormats.get(1)));
  }

  protected static final String EXECUTION_ENGINE_WRITE_METHOD =
      "executionEngineWriteMethodParameters";

  protected static Stream<Arguments> executionEngineWriteMethodParameters() {
    List<String> writeMethods =
        Arrays.asList(
            HiveBigQueryConfig.WRITE_METHOD_DIRECT, HiveBigQueryConfig.WRITE_METHOD_INDIRECT);
    Collections.shuffle(writeMethods);
    return Stream.of(
        Arguments.of("mr", writeMethods.get(0)), Arguments.of("tez", writeMethods.get(1)));
  }

  protected static final String EXECUTION_ENGINE_READ_FORMAT_WRITE_METHOD =
      "executionEngineReadFormatWriteMethodParameters";

  protected static Stream<Arguments> executionEngineReadFormatWriteMethodParameters() {
    List<String> readFormats = Arrays.asList(HiveBigQueryConfig.ARROW, HiveBigQueryConfig.AVRO);
    List<String> writeMethods =
        Arrays.asList(
            HiveBigQueryConfig.WRITE_METHOD_DIRECT, HiveBigQueryConfig.WRITE_METHOD_INDIRECT);
    Collections.shuffle(readFormats);
    Collections.shuffle(writeMethods);
    return Stream.of(
        Arguments.of("mr", readFormats.get(0), writeMethods.get(0)),
        Arguments.of("tez", readFormats.get(1), writeMethods.get(1)));
  }
}
