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
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestUtils {

  public static Logger logger = LoggerFactory.getLogger(TestUtils.class);
  public static final String HIVECONF_SYSTEM_OVERRIDE_PREFIX = "hiveconf_";
  public static final String LOCATION = "us";
  public static final String TEST_TABLE_NAME = "test";
  public static final String BIGLAKE_TABLE_NAME = "biglake";
  public static final String TEST_VIEW_NAME = "test_view";
  public static final String ANOTHER_TEST_TABLE_NAME = "another_test";
  public static final String ALL_TYPES_TABLE_NAME = "all_types";
  public static final String TIMESTAMP_TZ_TABLE_NAME = "timestamp_tz_table";
  public static final String MANAGED_TEST_TABLE_NAME = "managed_test";
  public static final String FIELD_TIME_PARTITIONED_TABLE_NAME = "field_time_partitioned";
  public static final String INGESTION_TIME_PARTITIONED_TABLE_NAME = "ingestion_time_partitioned";
  public static final String TEST_BUCKET_ENV_VAR = "TEST_BUCKET";

  // The BigLake bucket and connection must be created before running the tests.
  // Also, the connection's service account must be given permission to access the bucket.
  public static final String BIGLAKE_CONNECTION_ENV_VAR = "BIGLAKE_CONNECTION";
  public static final String BIGLAKE_BUCKET_ENV_VAR = "BIGLAKE_BUCKET";

  public static final String KMS_KEY_NAME_ENV_VAR = "BIGQUERY_KMS_KEY_NAME";

  public static String BIGQUERY_TEST_TABLE_DDL =
      String.join(
          "\n",
          "NUMBER INT64,", // Intentionally set this column uppercase to test Hive's case
          // insensitivity. See PR #98
          "text STRING");

  public static String BIGQUERY_ANOTHER_TEST_TABLE_DDL =
      String.join("\n", "num INT64,", "str_val STRING");

  public static String BIGQUERY_ALL_TYPES_TABLE_DDL =
      String.join(
          "\n",
          "tiny_int_val INT64 OPTIONS (description = 'A description for a TINYINT'),",
          "small_int_val INT64 OPTIONS (description = 'A description for a SMALLINT'),",
          "int_val INT64 OPTIONS (description = 'A description for a INT'),",
          "big_int_val INT64 OPTIONS (description = 'A description for a BIGINT'),",
          "bl BOOL OPTIONS (description = 'A description for a BOOLEAN'),",
          "fixed_char STRING OPTIONS (description = 'A description for a CHAR'),",
          "var_char STRING OPTIONS (description = 'A description for a VARCHAR'),",
          "str STRING OPTIONS (description = 'A description for a STRING'),",
          "day DATE OPTIONS (description = 'A description for a DATE'),",
          "ts DATETIME OPTIONS (description = 'A description for a TIMESTAMP'),",
          "bin BYTES OPTIONS (description = 'A description for a BINARY'),",
          "fl FLOAT64 OPTIONS (description = 'A description for a FLOAT'),",
          "dbl FLOAT64 OPTIONS (description = 'A description for a DOUBLE'),",
          "nums STRUCT<min NUMERIC, max NUMERIC, pi NUMERIC, big_pi NUMERIC> OPTIONS (description"
              + " = 'A description for a STRUCT'),",
          "int_arr ARRAY<int64> OPTIONS (description = 'A description for a ARRAY-BIGINT'),",
          "int_struct_arr ARRAY<STRUCT<i INT64>> OPTIONS (description = 'A description for a"
              + " ARRAY-STRUCT'),",
          "mixed_struct STRUCT<float_field FLOAT64, ts_field DATETIME> OPTIONS (description = 'A"
              + " description for a STRUCT-MIXED'),",
          "mp ARRAY<STRUCT<key STRING, value ARRAY<STRUCT<key STRING, value INT64>>>> OPTIONS"
              + " (description = 'A description for a MAP')");

  public static String BIGQUERY_TIMESTAMP_TZ_TABLE_DDL =
      String.join(
          "\n", "tstz TIMESTAMP OPTIONS (description = 'A description for a TIMESTAMPLOCALTZ')");

  public static String BIGQUERY_BIGLAKE_TABLE_CREATE_QUERY =
      String.join(
          "\n",
          "CREATE OR REPLACE EXTERNAL TABLE ${dataset}." + BIGLAKE_TABLE_NAME,
          "WITH CONNECTION `${project}.${location}.${connection}`",
          "OPTIONS (",
          "format = 'CSV',",
          "uris = ['gs://" + getBigLakeBucket() + "/test.csv']",
          ")");

  public static String HIVE_TEST_TABLE_DDL = String.join("\n", "number BIGINT,", "text STRING");

  public static String HIVE_TEST_VIEW_DDL = String.join("\n", "number BIGINT,", "text STRING");

  public static String HIVE_BIGLAKE_TABLE_DDL =
      String.join("\n", "a BIGINT,", "b BIGINT,", "c BIGINT");

  public static String HIVE_ANOTHER_TEST_TABLE_DDL =
      String.join("\n", "num BIGINT,", "str_val STRING");

  public static String HIVE_ALL_TYPES_TABLE_DDL =
      String.join(
          "\n",
          "tiny_int_val TINYINT COMMENT 'A description for a TINYINT',",
          "small_int_val SMALLINT COMMENT 'A description for a SMALLINT',",
          "int_val INT COMMENT 'A description for a INT',",
          "big_int_val BIGINT COMMENT 'A description for a BIGINT',",
          "bl BOOLEAN COMMENT 'A description for a BOOLEAN',",
          "fixed_char CHAR(10) COMMENT 'A description for a CHAR',",
          "var_char VARCHAR(10) COMMENT 'A description for a VARCHAR',",
          "str STRING COMMENT 'A description for a STRING',",
          "day DATE COMMENT 'A description for a DATE',",
          "ts TIMESTAMP COMMENT 'A description for a TIMESTAMP',",
          "bin BINARY COMMENT 'A description for a BINARY',",
          "fl FLOAT COMMENT 'A description for a FLOAT',",
          "dbl DOUBLE COMMENT 'A description for a DOUBLE',",
          "nums STRUCT<min: DECIMAL(38,9), max: DECIMAL(38,9), pi:"
              + " DECIMAL(38,9), big_pi: DECIMAL(38,9)> COMMENT 'A description for a STRUCT',",
          "int_arr ARRAY<BIGINT> COMMENT 'A description for a ARRAY-BIGINT',",
          "int_struct_arr ARRAY<STRUCT<i: BIGINT>> COMMENT 'A description for a ARRAY-STRUCT',",
          "mixed_struct STRUCT<float_field:FLOAT,ts_field:TIMESTAMP> COMMENT 'A description for a"
              + " STRUCT-MIXED',",
          "mp MAP<STRING,MAP<STRING,INT>> COMMENT 'A description for a MAP'");

  public static String HIVE_TIMESTAMP_TZ_TABLE_DDL =
      String.join("\n", "tstz TIMESTAMPLOCALTZ COMMENT 'A description for a TIMESTAMPLOCALTZ'");

  public static String HIVE_FIELD_TIME_PARTITIONED_TABLE_DDL =
      String.join("\n", "int_val BIGINT,", "ts TIMESTAMP");

  public static String HIVE_FIELD_TIME_PARTITIONED_TABLE_PROPS =
      String.join(
          "\n",
          "'bq.time.partition.field'='ts',",
          "'bq.time.partition.type'='MONTH',",
          "'bq.time.partition.expiration.ms'='2592000000',",
          "'bq.clustered.fields'='int_val'");

  public static String HIVE_INGESTION_TIME_PARTITIONED_DDL = String.join("\n", "int_val BIGINT");

  public static String HIVE_INGESTION_TIME_PARTITIONED_PROPS = "'bq.time.partition.type'='DAY'";

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
    Configuration conf = new Configuration();
    Map<String, String> hiveConfSystemOverrides = getHiveConfSystemOverrides();
    for (String key : hiveConfSystemOverrides.keySet()) {
      conf.set(key, hiveConfSystemOverrides.get(key));
    }
    Injector injector =
        Guice.createInjector(new BigQueryClientModule(), new HiveBigQueryConnectorModule(conf));
    BigQueryCredentialsSupplier credentialsSupplier =
        injector.getInstance(BigQueryCredentialsSupplier.class);
    return credentialsSupplier.getCredentials();
  }

  public static BigQueryClient getBigqueryClient() {
    Configuration conf = new Configuration();
    Map<String, String> hiveConfSystemOverrides = getHiveConfSystemOverrides();
    for (String key : hiveConfSystemOverrides.keySet()) {
      conf.set(key, hiveConfSystemOverrides.get(key));
    }
    Injector injector =
        Guice.createInjector(new BigQueryClientModule(), new HiveBigQueryConnectorModule(conf));
    return injector.getInstance(BigQueryClient.class);
  }

  public static String getProject() {
    return getBigqueryClient().getProjectId();
  }

  /**
   * The BigLake connection must be created prior to running the test, then its name must be set in
   * an environment variable, so we can retrieve it here during the test execution.
   */
  public static String getBigLakeConnection() {
    return System.getenv().getOrDefault(BIGLAKE_CONNECTION_ENV_VAR, "hive-integration-tests");
  }

  /**
   * The BigLake bucket must be created prior to running the test, then its name must be set in an
   * environment variable, so we can retrieve it here during the test execution.
   */
  public static String getBigLakeBucket() {
    return System.getenv().getOrDefault(BIGLAKE_BUCKET_ENV_VAR, getProject() + "-biglake-tests");
  }

  public static String getKmsKeyName() {
    String kmsKeyName = System.getenv().get(KMS_KEY_NAME_ENV_VAR);
    if (kmsKeyName == null) {
      throw new RuntimeException(KMS_KEY_NAME_ENV_VAR + " env var is not set");
    }
    return kmsKeyName;
  }

  /**
   * Returns the name of the bucket used to store temporary Avro files when testing the indirect
   * write method. This bucket is created automatically when running the tests.
   */
  public static String getTestBucket() {
    return System.getenv().getOrDefault(TEST_BUCKET_ENV_VAR, getProject() + "-integration-tests");
  }

  public static void createBqDataset(String dataset) {
    DatasetId datasetId = DatasetId.of(dataset);
    logger.warn("Creating test dataset: {}", datasetId);
    BigQuery bq =
        BigQueryOptions.newBuilder().setCredentials(getCredentials()).build().getService();
    bq.create(DatasetInfo.newBuilder(datasetId).setLocation(LOCATION).build());
  }

  public static void createOrReplaceLogicalView(String dataset, String table, String view) {
    String query =
        String.format(
            "CREATE OR REPLACE VIEW %s.%s AS (SELECT * FROM %s.%s)", dataset, view, dataset, table);
    getBigqueryClient().query(query);
  }

  /**
   * Create a BQ materialized view
   * (https://cloud.google.com/bigquery/docs/materialized-views-intro). Note: Replacing an existing
   * materialized view is not supported by BigQuery, so we cannot use a `CREATE OR REPLACE`
   * statement.
   */
  public static void createMaterializedView(String dataset, String table, String view) {
    String query =
        String.format(
            "CREATE MATERIALIZED VIEW %s.%s AS (SELECT * FROM %s.%s)",
            dataset, view, dataset, table);
    getBigqueryClient().query(query);
  }

  public static void dropBqTableIfExists(String dataset, String table) {
    TableId tableId = TableId.of(getProject(), dataset, table);
    if (getBigqueryClient().tableExists(tableId)) {
      getBigqueryClient().deleteTable(tableId);
      try {
        // Wait a bit to avoid rate limiting issues with the BQ backend for table operations
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
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

  public static List<Blob> getBlobs(String bucketName, String dir) {
    return Lists.newArrayList(
        getStorageClient().list(bucketName, BlobListOption.prefix(dir)).iterateAll());
  }

  public static void emptyBucket(String bucketName) {
    deleteBlobs(getBlobs(bucketName));
  }

  public static void emptyGcsDir(String bucketName, String dir) {
    deleteBlobs(getBlobs(bucketName, dir));
  }

  private static void deleteBlobs(List<Blob> blobs) {
    if (blobs.size() == 0) {
      System.err.println("No blobs to delete");
      return;
    }

    StorageBatch batch = getStorageClient().batch();
    for (Blob blob : blobs) {
      System.err.println("Deleting " + blob.getName());
      batch.delete(blob.getBlobId());
    }
    batch.submit();
  }
}
