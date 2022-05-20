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
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.storage.*;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import repackaged.by.hivebqconnector.com.google.common.cache.Cache;
import repackaged.by.hivebqconnector.com.google.common.cache.CacheBuilder;
import repackaged.by.hivebqconnector.com.google.common.collect.ImmutableMap;
import repackaged.by.hivebqconnector.com.google.common.collect.Lists;

public class TestUtils {

  public static Logger logger = LoggerFactory.getLogger(TestUtils.class);

  public static final String DATASET = "integration";
  public static final String TEST_TABLE_NAME = "test";
  public static final String ANOTHER_TEST_TABLE_NAME = "another_test";
  public static final String ALL_TYPES_TABLE_NAME = "all_types";
  public static final String TEMP_BUCKET_NAME = getProject() + "-integration";
  public static final String TEMP_GCS_PATH = "gs://" + TEMP_BUCKET_NAME + "/temp";

  public static String BIGQUERY_TEST_TABLE_CREATE_QUERY =
      Stream.of(
              "CREATE TABLE " + DATASET + "." + TEST_TABLE_NAME + " (",
              "number INT64,",
              "text STRING",
              ")")
          .collect(Collectors.joining("\n"));

  public static String BIGQUERY_ANOTHER_TEST_TABLE_CREATE_QUERY =
      Stream.of(
              "CREATE TABLE " + DATASET + "." + ANOTHER_TEST_TABLE_NAME + " (",
              "num INT64,",
              "str_val STRING",
              ")")
          .collect(Collectors.joining("\n"));

  public static String BIGQUERY_ALL_TYPES_TABLE_CREATE_QUERY =
      Stream.of(
              "CREATE TABLE " + DATASET + "." + ALL_TYPES_TABLE_NAME + " (",
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
              "CREATE TABLE " + TEST_TABLE_NAME + " (",
              "number BIGINT,",
              "text STRING",
              ")",
              "STORED BY" + " 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'",
              "TBLPROPERTIES (",
              "  'bq.project'='" + getProject() + "',",
              "  'bq.dataset'='" + DATASET + "',",
              "  'bq.table'='" + TEST_TABLE_NAME + "'",
              ");")
          .collect(Collectors.joining("\n"));

  public static String HIVE_ANOTHER_TEST_TABLE_CREATE_QUERY =
      Stream.of(
              "CREATE TABLE " + ANOTHER_TEST_TABLE_NAME + " (",
              "num BIGINT,",
              "str_val STRING",
              ")",
              "STORED BY" + " 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'",
              "TBLPROPERTIES (",
              "  'bq.project'='" + getProject() + "',",
              "  'bq.dataset'='" + DATASET + "',",
              "  'bq.table'='" + ANOTHER_TEST_TABLE_NAME + "'",
              ");")
          .collect(Collectors.joining("\n"));

  public static String HIVE_ALL_TYPES_TABLE_CREATE_QUERY =
      Stream.of(
              "CREATE TABLE " + ALL_TYPES_TABLE_NAME + " (",
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
              "  'bq.project'='" + getProject() + "',",
              "  'bq.dataset'='" + DATASET + "',",
              "  'bq.table'='" + ALL_TYPES_TABLE_NAME + "'",
              ");")
          .collect(Collectors.joining("\n"));

  private static final Cache<String, TableInfo> destinationTableCache =
      CacheBuilder.newBuilder().expireAfterWrite(15, TimeUnit.MINUTES).maximumSize(1000).build();

  public static BigQuery getBigquery() {
    return BigQueryOptions.getDefaultInstance().getService();
  }

  public static String getProject() {
    return getBigquery().getOptions().getProjectId();
  }

  public static void createDataset(String dataset) {
    BigQuery bq = getBigquery();
    DatasetId datasetId = DatasetId.of(dataset);
    logger.warn("Creating test dataset: {}", datasetId);
    bq.create(DatasetInfo.of(datasetId));
  }

  public static void createBigQueryTable(String dataset, String table, Schema schema) {
    BigQuery bq = getBigquery();
    TableId tableId = TableId.of(dataset, table);
    TableDefinition tableDefinition = StandardTableDefinition.of(schema);
    TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
    bq.create(tableInfo);
  }

  public static TableResult runBqQuery(String query) {
    BigQueryClient bigQueryClient =
        new BigQueryClient(
            getBigquery(),
            Optional.empty(),
            Optional.empty(),
            destinationTableCache,
            ImmutableMap.of());
    return bigQueryClient.query(query);
  }

  public static void deleteDatasetAndTables(String dataset) {
    BigQuery bq = getBigquery();
    logger.warn("Deleting test dataset '{}' and its contents", dataset);
    bq.delete(DatasetId.of(dataset), BigQuery.DatasetDeleteOption.deleteContents());
  }

  public static void createBucket(String bucketName) {
    Storage storage = StorageOptions.newBuilder().build().getService();
    storage.create(BucketInfo.newBuilder(bucketName).build());
  }

  public static void deleteBucket(String bucketName) {
    Storage storage = StorageOptions.newBuilder().build().getService();
    Iterable<Blob> blobs = storage.list(bucketName).iterateAll();
    for (Blob blob : blobs) {
      blob.delete();
    }
    Bucket bucket = storage.get(bucketName);
    bucket.delete();
  }

  public static List<Blob> getBlobs(String bucketName) {
    Storage storage = StorageOptions.newBuilder().build().getService();
    return Lists.newArrayList(storage.list(bucketName).iterateAll());
  }
}
