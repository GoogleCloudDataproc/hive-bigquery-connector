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

import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientModule;
import com.google.cloud.bigquery.connector.common.BigQueryCredentialsSupplier;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConnectorModule;
import com.google.cloud.storage.*;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import repackaged.by.hivebqconnector.com.google.common.collect.Lists;

public class TestUtils {

  public static Logger logger = LoggerFactory.getLogger(TestUtils.class);
  public static final String HIVECONF_SYSTEM_OVERRIDE_PREFIX = "hiveconf_";
  public static final String LOCATION = "us";
  public static final String TEST_TABLE_NAME = "test";
  public static final String BIGLAKE_TABLE_NAME = "biglake";
  public static final String TEST_VIEW_NAME = "test_view";
  public static final String ANOTHER_TEST_TABLE_NAME = "another_test";
  public static final String ALL_TYPES_TABLE_NAME = "all_types";
  public static final String MANAGED_TEST_TABLE_NAME = "managed_test";
  public static final String FIELD_TIME_PARTITIONED_TABLE_NAME = "field_time_partitioned";
  public static final String INGESTION_TIME_PARTITIONED_TABLE_NAME = "ingestion_time_partitioned";
  public static final String INDIRECT_WRITE_BUCKET_NAME_ENV_VAR = "INDIRECT_WRITE_BUCKET";
  public static final String TEMP_GCS_PATH = "gs://" + getIndirectWriteBucket() + "/temp";

  // The BigLake bucket and connection must be created before running the tests.
  // Also, the connection's service account must be given permission to access the bucket.
  public static final String BIGLAKE_CONNECTION = "hive-integration-tests";
  public static final String BIGLAKE_BUCKET_NAME_ENV_VAR = "BIGLAKE_BUCKET";

  public static String BIGQUERY_TEST_TABLE_CREATE_QUERY =
      Stream.of(
              "CREATE OR REPLACE TABLE ${dataset}." + TEST_TABLE_NAME + " (",
              "number INT64,",
              "text STRING",
              ")")
          .collect(Collectors.joining("\n"));

  public static String BIGQUERY_ANOTHER_TEST_TABLE_CREATE_QUERY =
      Stream.of(
              "CREATE OR REPLACE TABLE ${dataset}." + ANOTHER_TEST_TABLE_NAME + " (",
              "num INT64,",
              "str_val STRING",
              ")")
          .collect(Collectors.joining("\n"));

  public static String BIGQUERY_ALL_TYPES_TABLE_CREATE_QUERY =
      Stream.of(
              "CREATE OR REPLACE TABLE ${dataset}." + ALL_TYPES_TABLE_NAME + " (",
              "tiny_int_val INT64,",
              "small_int_val INT64,",
              "int_val INT64,",
              "big_int_val INT64,",
              "bl BOOL,",
              "fixed_char STRING,",
              "var_char STRING,",
              "str STRING,",
              "day DATE,",
              "ts TIMESTAMP,",
              "bin BYTES,",
              "fl FLOAT64,",
              "dbl FLOAT64,",
              "nums STRUCT<min BIGNUMERIC, max BIGNUMERIC, pi BIGNUMERIC, big_pi BIGNUMERIC>,",
              "int_arr ARRAY<int64>,",
              "int_struct_arr ARRAY<STRUCT<i INT64>>",
              ")")
          .collect(Collectors.joining("\n"));

  public static String BIGQUERY_BIGLAKE_TABLE_CREATE_QUERY =
      Stream.of(
              "CREATE OR REPLACE EXTERNAL TABLE ${dataset}." + BIGLAKE_TABLE_NAME,
              "WITH CONNECTION `${project}.${location}.${connection}`",
              "OPTIONS (",
              "format = 'CSV',",
              "uris = ['gs://" + getBigLakeBucket() + "/test.csv']",
              ")")
          .collect(Collectors.joining("\n"));

  public static String BIGQUERY_MANAGED_TEST_TABLE_CREATE_QUERY =
      Stream.of(
              "CREATE OR REPLACE TABLE ${dataset}." + MANAGED_TEST_TABLE_NAME + " (",
              "tiny_int_val INT64,",
              "small_int_val INT64,",
              "int_val INT64,",
              "big_int_val INT64,",
              "bl BOOL,",
              "fixed_char STRING,",
              "var_char STRING,",
              "str STRING,",
              "day DATE,",
              "ts TIMESTAMP,",
              "bin BYTES,",
              "fl FLOAT64,",
              "dbl FLOAT64,",
              "nums STRUCT<min BIGNUMERIC, max BIGNUMERIC, pi BIGNUMERIC, big_pi BIGNUMERIC>,",
              "int_arr ARRAY<int64>,",
              "int_struct_arr ARRAY<STRUCT<i INT64>>",
              ")")
          .collect(Collectors.joining("\n"));

  public static String HIVE_TEST_TABLE_CREATE_QUERY =
      Stream.of(
              "CREATE EXTERNAL TABLE " + TEST_TABLE_NAME + " (",
              "number BIGINT,",
              "text STRING",
              ")",
              "STORED BY" + " 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'",
              "TBLPROPERTIES (",
              "  'bq.project'='${project}',",
              "  'bq.dataset'='${dataset}',",
              "  'bq.table'='" + TEST_TABLE_NAME + "'",
              ");")
          .collect(Collectors.joining("\n"));

  public static String HIVE_TEST_VIEW_CREATE_QUERY =
      Stream.of(
              "CREATE EXTERNAL TABLE " + TEST_VIEW_NAME + " (",
              "number BIGINT,",
              "text STRING",
              ")",
              "STORED BY" + " 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'",
              "TBLPROPERTIES (",
              "  'bq.project'='${project}',",
              "  'bq.dataset'='${dataset}',",
              "  'bq.table'='" + TEST_VIEW_NAME + "'",
              ");")
          .collect(Collectors.joining("\n"));

  public static String HIVE_BIGLAKE_TABLE_CREATE_QUERY =
      Stream.of(
              "CREATE EXTERNAL TABLE " + BIGLAKE_TABLE_NAME + " (",
              "a BIGINT,",
              "b BIGINT,",
              "c BIGINT",
              ")",
              "STORED BY" + " 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'",
              "TBLPROPERTIES (",
              "  'bq.project'='${project}',",
              "  'bq.dataset'='${dataset}',",
              "  'bq.table'='" + BIGLAKE_TABLE_NAME + "'",
              ");")
          .collect(Collectors.joining("\n"));

  public static String HIVE_ANOTHER_TEST_TABLE_CREATE_QUERY =
      Stream.of(
              "CREATE EXTERNAL TABLE " + ANOTHER_TEST_TABLE_NAME + " (",
              "num BIGINT,",
              "str_val STRING",
              ")",
              "STORED BY" + " 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'",
              "TBLPROPERTIES (",
              "  'bq.project'='${project}',",
              "  'bq.dataset'='${dataset}',",
              "  'bq.table'='" + ANOTHER_TEST_TABLE_NAME + "'",
              ");")
          .collect(Collectors.joining("\n"));

  public static String HIVE_ALL_TYPES_TABLE_CREATE_QUERY =
      Stream.of(
              "CREATE EXTERNAL TABLE " + ALL_TYPES_TABLE_NAME + " (",
              "tiny_int_val TINYINT,",
              "small_int_val SMALLINT,",
              "int_val INT,",
              "big_int_val BIGINT,",
              "bl BOOLEAN,",
              "fixed_char CHAR(10),",
              "var_char VARCHAR(10),",
              "str STRING,",
              "day DATE,",
              "ts TIMESTAMP,",
              "bin BINARY,",
              "fl FLOAT,",
              "dbl DOUBLE,",
              "nums STRUCT<min: DECIMAL(38,10), max: DECIMAL(38,10), pi:"
                  + " DECIMAL(38,10), big_pi: DECIMAL(38,10)>,",
              "int_arr ARRAY<BIGINT>,",
              "int_struct_arr ARRAY<STRUCT<i: BIGINT>>",
              ")",
              "STORED BY" + " 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'",
              "TBLPROPERTIES (",
              "  'bq.project'='${project}',",
              "  'bq.dataset'='${dataset}',",
              "  'bq.table'='" + ALL_TYPES_TABLE_NAME + "'",
              ");")
          .collect(Collectors.joining("\n"));

  public static String HIVE_MANAGED_TEST_TABLE_CREATE_QUERY =
      Stream.of(
              "CREATE TABLE " + MANAGED_TEST_TABLE_NAME + " (",
              "tiny_int_val TINYINT,",
              "small_int_val SMALLINT,",
              "int_val INT,",
              "big_int_val BIGINT,",
              "bl BOOLEAN,",
              "fixed_char CHAR(10),",
              "var_char VARCHAR(10),",
              "str STRING,",
              "day DATE,",
              "ts TIMESTAMP,",
              "bin BINARY,",
              "fl FLOAT,",
              "dbl DOUBLE,",
              "nums STRUCT<min: DECIMAL(38,9), max: DECIMAL(38,9), pi:"
                  + " DECIMAL(38,9), big_pi: DECIMAL(38,9)>,",
              "int_arr ARRAY<BIGINT>,",
              "int_struct_arr ARRAY<STRUCT<i: BIGINT>>",
              ")",
              "STORED BY" + " 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'",
              "TBLPROPERTIES (",
              "  'bq.project'='${project}',",
              "  'bq.dataset'='${dataset}',",
              "  'bq.table'='" + MANAGED_TEST_TABLE_NAME + "'",
              ");")
          .collect(Collectors.joining("\n"));

  public static String HIVE_FIELD_TIME_PARTITIONED_TABLE_CREATE_QUERY =
      Stream.of(
              "CREATE TABLE " + FIELD_TIME_PARTITIONED_TABLE_NAME + " (",
              "int_val BIGINT,",
              "ts TIMESTAMP",
              ")",
              "STORED BY" + " 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'",
              "TBLPROPERTIES (",
              "  'bq.project'='${project}',",
              "  'bq.dataset'='${dataset}',",
              "  'bq.table'='" + FIELD_TIME_PARTITIONED_TABLE_NAME + "',",
              "  'bq.time.partition.field'='ts',",
              "  'bq.time.partition.type'='MONTH',",
              "  'bq.time.partition.expiration.ms'='2592000000',",
              "  'bq.clustered.fields'='int_val'",
              ");")
          .collect(Collectors.joining("\n"));

  public static String HIVE_INGESTION_TIME_PARTITIONED_TABLE_CREATE_QUERY =
      Stream.of(
              "CREATE TABLE " + INGESTION_TIME_PARTITIONED_TABLE_NAME + " (",
              "int_val BIGINT",
              ")",
              "STORED BY" + " 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'",
              "TBLPROPERTIES (",
              "  'bq.project'='${project}',",
              "  'bq.dataset'='${dataset}',",
              "  'bq.table'='" + INGESTION_TIME_PARTITIONED_TABLE_NAME + "',",
              "  'bq.time.partition.type'='DAY'",
              ");")
          .collect(Collectors.joining("\n"));

  /** Return Hive config values passed from system properties */
  public static Map<String, String> getHiveConfSystemOverrides() {
    Map<String, String> overrides = new HashMap<>();
    Properties systemProperties = System.getProperties();
    for (String key : systemProperties.stringPropertyNames()) {
      if (key.startsWith(HIVECONF_SYSTEM_OVERRIDE_PREFIX)) {
        String hiveConfKey = key.substring(HIVECONF_SYSTEM_OVERRIDE_PREFIX.length());
        overrides.put(hiveConfKey, systemProperties.getProperty(key));
      }
    }
    return overrides;
  }

  private static com.google.auth.Credentials getCredentials() {
    Configuration config = new Configuration();
    Map<String, String> hiveConfSystemOverrides = getHiveConfSystemOverrides();
    for (String key : hiveConfSystemOverrides.keySet()) {
      config.set(key, hiveConfSystemOverrides.get(key));
    }
    Injector injector =
        Guice.createInjector(new BigQueryClientModule(), new HiveBigQueryConnectorModule(config));
    BigQueryCredentialsSupplier credentialsSupplier =
        injector.getInstance(BigQueryCredentialsSupplier.class);
    return credentialsSupplier.getCredentials();
  }

  public static BigQueryClient getBigqueryClient() {
    Configuration config = new Configuration();
    Map<String, String> hiveConfSystemOverrides = getHiveConfSystemOverrides();
    for (String key : hiveConfSystemOverrides.keySet()) {
      config.set(key, hiveConfSystemOverrides.get(key));
    }
    Injector injector =
        Guice.createInjector(new BigQueryClientModule(), new HiveBigQueryConnectorModule(config));
    return injector.getInstance(BigQueryClient.class);
  }

  public static String getProject() {
    return getBigqueryClient().getProjectId();
  }

  /**
   * The BigLake bucket must be created prior to running the test, then its name must be set in an
   * environment variable, so we can retrieve it here during the test execution.
   */
  public static String getBigLakeBucket() {
    return System.getenv()
        .getOrDefault(BIGLAKE_BUCKET_NAME_ENV_VAR, getProject() + "-biglake-tests");
  }

  /**
   * Returns the name of the bucket used to store temporary Avro files when testing the indirect
   * write method. This bucket is created automatically when running the tests.
   */
  public static String getIndirectWriteBucket() {
    return System.getenv()
        .getOrDefault(INDIRECT_WRITE_BUCKET_NAME_ENV_VAR, getProject() + "-indirect-write-tests");
  }

  public static void createBqDataset(String dataset) {
    DatasetId datasetId = DatasetId.of(dataset);
    logger.warn("Creating test dataset: {}", datasetId);
    BigQuery bq =
        BigQueryOptions.newBuilder().setCredentials(getCredentials()).build().getService();
    bq.create(DatasetInfo.newBuilder(datasetId).setLocation(LOCATION).build());
  }

  public static void createOrReplaceBqView(String dataset, String table, String view) {
    String query =
        String.format(
            "CREATE OR REPLACE VIEW %s.%s AS (SELECT * FROM %s.%s)", dataset, view, dataset, table);
    getBigqueryClient().query(query);
  }

  public static void dropBqTableIfExists(String dataset, String table) {
    TableId tableId = TableId.of(dataset, table);
    getBigqueryClient().deleteTable(tableId);
  }

  public static boolean bQTableExists(String dataset, String tableName) {
    return getBigqueryClient().tableExists(TableId.of(getProject(), dataset, tableName));
  }

  public static TableInfo getTableInfo(String dataset, String tableName) {
    return getBigqueryClient().getTable(TableId.of(getProject(), dataset, tableName));
  }

  public static void deleteBqDatasetAndTables(String dataset) {
    BigQuery bq =
        BigQueryOptions.newBuilder().setCredentials(getCredentials()).build().getService();
    logger.warn("Deleting test dataset '{}' and its contents", dataset);
    bq.delete(DatasetId.of(dataset), BigQuery.DatasetDeleteOption.deleteContents());
  }

  private static Storage getStorageClient() {
    return StorageOptions.newBuilder().setCredentials(getCredentials()).build().getService();
  }

  public static void createBucket(String bucketName) {
    getStorageClient().create(BucketInfo.newBuilder(bucketName).setLocation(LOCATION).build());
  }

  public static void uploadBlob(String bucketName, String objectName, byte[] contents) {
    BlobId blobId = BlobId.of(bucketName, objectName);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    getStorageClient().create(blobInfo, contents);
  }

  public static void deleteBucket(String bucketName) {
    Storage storage = getStorageClient();
    Iterable<Blob> blobs = storage.list(bucketName).iterateAll();
    for (Blob blob : blobs) {
      blob.delete();
    }
    Bucket bucket = storage.get(bucketName);
    bucket.delete();
  }

  public static List<Blob> getBlobs(String bucketName) {
    return Lists.newArrayList(getStorageClient().list(bucketName).iterateAll());
  }

  public static void emptyBucket(String bucketName) {
    List<Blob> blobs = getBlobs(bucketName);
    if (blobs.size() > 0) {
      StorageBatch batch = getStorageClient().batch();
      for (Blob blob : blobs) {
        batch.delete(blob.getBlobId());
      }
      batch.submit();
    }
  }
}
