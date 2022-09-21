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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
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
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import repackaged.by.hivebqconnector.com.google.common.cache.Cache;
import repackaged.by.hivebqconnector.com.google.common.cache.CacheBuilder;
import repackaged.by.hivebqconnector.com.google.common.collect.ImmutableMap;
import repackaged.by.hivebqconnector.com.google.common.collect.Lists;

public class TestUtils {

  public static Logger logger = LoggerFactory.getLogger(TestUtils.class);
  public final static String HIVECONF_SYSTEM_OVERRIDE_PREFIX = "hiveconf_";
  public static final String LOCATION = "us";
  public static final String TEST_TABLE_NAME = "test";
  public static final String ANOTHER_TEST_TABLE_NAME = "another_test";
  public static final String ALL_TYPES_TABLE_NAME = "all_types";
  public static final String MANAGED_TEST_TABLE_NAME = "managed_test";
  public static final String TEMP_BUCKET_NAME = getProject() + "-integration";
  public static final String TEMP_GCS_PATH = "gs://" + TEMP_BUCKET_NAME + "/temp";

  public static String BIGQUERY_TEST_TABLE_CREATE_QUERY =
      Stream.of(
              "CREATE TABLE ${dataset}." + TEST_TABLE_NAME + " (",
              "number INT64,",
              "text STRING",
              ")")
          .collect(Collectors.joining("\n"));

  public static String BIGQUERY_ANOTHER_TEST_TABLE_CREATE_QUERY =
      Stream.of(
              "CREATE TABLE ${dataset}." + ANOTHER_TEST_TABLE_NAME + " (",
              "num INT64,",
              "str_val STRING",
              ")")
          .collect(Collectors.joining("\n"));

  public static String BIGQUERY_ALL_TYPES_TABLE_CREATE_QUERY =
      Stream.of(
              "CREATE TABLE ${dataset}." + ALL_TYPES_TABLE_NAME + " (",
              "int_val INT64,",
              "bl BOOL,",
              "str STRING,",
              "day DATE,",
              "ts TIMESTAMP,",
              "bin BYTES,",
              "fl FLOAT64,",
              "nums STRUCT<min NUMERIC, max NUMERIC, pi NUMERIC, big_pi NUMERIC>,",
              "int_arr ARRAY<int64>,",
              "int_struct_arr ARRAY<STRUCT<i INT64>>",
              ")")
          .collect(Collectors.joining("\n"));

  public static String BIGQUERY_MANAGED_TEST_TABLE_CREATE_QUERY =
      Stream.of(
              "CREATE TABLE ${dataset}." + MANAGED_TEST_TABLE_NAME + " (",
              "int_val INT64,",
              "bl BOOL,",
              "str STRING,",
              "day DATE,",
              "ts TIMESTAMP,",
              "bin BYTES,",
              "fl FLOAT64,",
              "nums STRUCT<min NUMERIC, max NUMERIC, pi NUMERIC, big_pi NUMERIC>,",
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
              "int_val BIGINT,",
              "bl BOOLEAN,",
              "str STRING,",
              "day DATE,",
              "ts TIMESTAMP,",
              "bin BINARY,",
              "fl DOUBLE,",
              "nums STRUCT<min: DECIMAL(38,9), max: DECIMAL(38,9), pi:"
                  + " DECIMAL(38,9), big_pi: DECIMAL(38,9)>,",
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
              "int_val BIGINT,",
              "bl BOOLEAN,",
              "str STRING,",
              "day DATE,",
              "ts TIMESTAMP,",
              "bin BINARY,",
              "fl DOUBLE,",
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

  public static final Cache<String, TableInfo> destinationTableCache =
      CacheBuilder.newBuilder().expireAfterWrite(15, TimeUnit.MINUTES).maximumSize(1000).build();

  /**
   * Return Hive config values passed from system properties
   */
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
    Injector injector = Guice.createInjector(
        new BigQueryClientModule(),
        new HiveBigQueryConnectorModule(config));
    BigQueryCredentialsSupplier credentialsSupplier = injector.getInstance(BigQueryCredentialsSupplier.class);
    return credentialsSupplier.getCredentials();
  }

  public static BigQuery getBigquery() {
    return BigQueryOptions.newBuilder().setCredentials(getCredentials()).build().getService();
  }

  public static String getProject() {
    return getBigquery().getOptions().getProjectId();
  }

  public static void createDataset(String dataset) {
    BigQuery bq = getBigquery();
    DatasetId datasetId = DatasetId.of(dataset);
    logger.warn("Creating test dataset: {}", datasetId);
    bq.create(DatasetInfo.newBuilder(datasetId).setLocation(LOCATION).build());
  }

  public static boolean bQTableExists(String dataset, String tableName) {
    BigQueryClient bigQueryClient =
        new BigQueryClient(
            getBigquery(),
            Optional.empty(),
            Optional.empty(),
            destinationTableCache,
            ImmutableMap.of());
    return bigQueryClient.tableExists(TableId.of(getProject(), dataset, tableName));
  }

  public static TableInfo getTableInfo(String dataset, String tableName) {
    BigQueryClient bigQueryClient =
        new BigQueryClient(
            getBigquery(),
            Optional.empty(),
            Optional.empty(),
            destinationTableCache,
            ImmutableMap.of());
    return bigQueryClient.getTable(TableId.of(getProject(), dataset, tableName));
  }

  public static void deleteDatasetAndTables(String dataset) {
    BigQuery bq = getBigquery();
    logger.warn("Deleting test dataset '{}' and its contents", dataset);
    bq.delete(DatasetId.of(dataset), BigQuery.DatasetDeleteOption.deleteContents());
  }

  private static Storage getStorageClient() {
    return StorageOptions.newBuilder().setCredentials(getCredentials()).build().getService();
  }

  public static void createBucket(String bucketName) {
    getStorageClient().create(BucketInfo.newBuilder(bucketName).setLocation(LOCATION).build());
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
}
