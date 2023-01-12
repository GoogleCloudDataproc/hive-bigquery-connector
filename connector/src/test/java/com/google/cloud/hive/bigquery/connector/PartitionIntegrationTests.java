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

import com.google.cloud.bigquery.*;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import repackaged.by.hivebqconnector.com.google.common.collect.ImmutableList;
import repackaged.by.hivebqconnector.com.google.common.collect.Streams;

public class PartitionIntegrationTests extends IntegrationTestsBase {

  @Test
  public void testFieldTimePartition() {
    initHive();
    // Make sure the BQ table doesn't exist
    dropBqTableIfExists(dataset, FIELD_TIME_PARTITIONED_TABLE_NAME);
    // Create the table using Hive
    createManagedTable(
        FIELD_TIME_PARTITIONED_TABLE_NAME,
        HIVE_FIELD_TIME_PARTITIONED_TABLE_DDL,
        HIVE_FIELD_TIME_PARTITIONED_TABLE_PROPS,
        null);
    // Verify that the BQ table has the right partition & clustering options
    StandardTableDefinition tableDef =
        getTableInfo(dataset, FIELD_TIME_PARTITIONED_TABLE_NAME).getDefinition();
    TimePartitioning timePartitioning = tableDef.getTimePartitioning();
    assertEquals(2592000000L, Objects.requireNonNull(timePartitioning).getExpirationMs());
    assertEquals("ts", timePartitioning.getField());
    assertEquals(TimePartitioning.Type.MONTH, timePartitioning.getType());
    Clustering clustering = tableDef.getClustering();
    assertEquals(ImmutableList.of("int_val"), Objects.requireNonNull(clustering).getFields());
  }

  @CartesianTest
  public void testInsertIntoPartition(
      @CartesianTest.Values(strings = {"mr", "tez"}) String engine,
      @CartesianTest.Values(
              strings = {
                HiveBigQueryConfig.WRITE_METHOD_DIRECT,
                HiveBigQueryConfig.WRITE_METHOD_INDIRECT
              })
          String writeMethod) {
    hive.setHiveConfValue(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    initHive(engine, HiveBigQueryConfig.ARROW);
    // Make sure the BQ table doesn't exist
    dropBqTableIfExists(dataset, PARTITION_CLAUSE_TABLE_NAME);
    // Create the table using Hive
    runHiveScript(HIVE_PARTITION_CLAUSE_TABLE_CREATE_QUERY);
    if (engine.equals("tez")) {
      // TODO: Add support for Tez
      Throwable exception =
          assertThrows(
              RuntimeException.class,
              () ->
                  runHiveScript(
                      String.join(
                          "\n",
                          "INSERT INTO TABLE "
                              + PARTITION_CLAUSE_TABLE_NAME
                              + " PARTITION(dt='2022-12-01') VALUES(",
                          "888",
                          ");")));
      return;
    }

    // Insert using "INTO TABLE ... PARTITION()" statement
    runHiveScript(
        String.join(
            "\n",
            "INSERT INTO TABLE "
                + PARTITION_CLAUSE_TABLE_NAME
                + " PARTITION(dt='2022-12-01') VALUES(",
            "888",
            ");"));
    // Insert using "INTO TABLE" statement (i.e. without "PARTITION")
    runHiveScript(
        String.join(
            "\n",
            "INSERT INTO TABLE " + PARTITION_CLAUSE_TABLE_NAME + " VALUES(",
            "999, '2023-01-03'",
            ");"));

    // TODO: Insert with dynamic partitioning

    // Read the data using the BQ SDK
    TableResult result =
        runBqQuery(
            String.format(
                "SELECT * FROM `${dataset}.%s` ORDER BY int_val", PARTITION_CLAUSE_TABLE_NAME));
    // Verify we get the expected values
    assertEquals(2, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    assertEquals(888L, rows.get(0).get(0).getLongValue());
    assertEquals("2022-12-01", rows.get(0).get(1).getStringValue());
    assertEquals(999L, rows.get(1).get(0).getLongValue());
    assertEquals("2023-01-03", rows.get(1).get(1).getStringValue());
  }

  @CartesianTest
  public void testInsertOverwritePartition(
      @CartesianTest.Values(strings = {"mr", "tez"}) String engine,
      @CartesianTest.Values(
              strings = {
                HiveBigQueryConfig.WRITE_METHOD_DIRECT,
                HiveBigQueryConfig.WRITE_METHOD_INDIRECT
              })
          String writeMethod) {
    hive.setHiveConfValue(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    initHive(engine, HiveBigQueryConfig.ARROW);
    // Make sure the BQ table doesn't exist
    dropBqTableIfExists(dataset, PARTITION_CLAUSE_TABLE_NAME);
    // Create the table using Hive
    runHiveScript(HIVE_PARTITION_CLAUSE_TABLE_CREATE_QUERY);
    // Add some initial data
    runBqQuery(
        String.format(
            "INSERT `${dataset}.%s` VALUES (123, '2019-08-15'), (888, '2022-12-01'), (999,"
                + " '2022-12-01')",
            PARTITION_CLAUSE_TABLE_NAME));
    if (engine.equals("tez") || writeMethod.equals(HiveBigQueryConfig.WRITE_METHOD_INDIRECT)) {
      // TODO: Add support for Tez and indirect write method
      Throwable exception =
          assertThrows(
              RuntimeException.class,
              () ->
                  runHiveScript(
                      String.join(
                          "\n",
                          "INSERT OVERWRITE TABLE "
                              + PARTITION_CLAUSE_TABLE_NAME
                              + " PARTITION(dt='2022-12-01') VALUES(",
                          "777",
                          ");")));
      return;
    }
    // Overwrite the partition using Hive
    runHiveScript(
        String.join(
            "\n",
            "INSERT OVERWRITE TABLE "
                + PARTITION_CLAUSE_TABLE_NAME
                + " PARTITION(dt='2022-12-01') VALUES(",
            "777",
            ");"));
    TableResult result =
        runBqQuery(
            String.format(
                "SELECT * FROM `${dataset}.%s` ORDER BY int_val", PARTITION_CLAUSE_TABLE_NAME));
    // Verify we get the expected values
    assertEquals(2, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    assertEquals(123L, rows.get(0).get(0).getLongValue());
    assertEquals("2019-08-15", rows.get(0).get(1).getStringValue());
    assertEquals(777L, rows.get(1).get(0).getLongValue());
    assertEquals("2022-12-01", rows.get(1).get(1).getStringValue());
  }

  @Test
  public void testCreateIngestionTimePartition() {
    initHive();
    // Make sure the BQ table doesn't exist
    dropBqTableIfExists(dataset, INGESTION_TIME_PARTITIONED_TABLE_NAME);
    // Create the table using Hive
    createManagedTable(
        INGESTION_TIME_PARTITIONED_TABLE_NAME,
        HIVE_INGESTION_TIME_PARTITIONED_DDL,
        HIVE_INGESTION_TIME_PARTITIONED_PROPS,
        null);
    // Retrieve the table metadata from BigQuery
    StandardTableDefinition tableDef =
        getTableInfo(dataset, INGESTION_TIME_PARTITIONED_TABLE_NAME).getDefinition();
    TimePartitioning timePartitioning = tableDef.getTimePartitioning();
    assertEquals(TimePartitioning.Type.DAY, timePartitioning.getType());
    assertNull(timePartitioning.getField());
    List<Object[]> rows =
        hive.executeStatement("DESCRIBE " + INGESTION_TIME_PARTITIONED_TABLE_NAME);
    // Verify that the partition pseudo columns were added.
    // Note: There is apparently a bug in Hive where the DESCRIBE command
    // returns "from deserializer" for the comment (i.e. description) of
    // every column.
    assertArrayEquals(
        new Object[] {
          new Object[] {"int_val", "bigint", "from deserializer"},
          new Object[] {"_partitiontime", "timestamp", "from deserializer"},
          new Object[] {"_partitiondate", "date", "from deserializer"}
        },
        rows.toArray());
  }

  @Test
  public void testQueryIngestionTimePartition() {
    initHive();
    // Make sure the BQ table doesn't exist
    dropBqTableIfExists(dataset, INGESTION_TIME_PARTITIONED_TABLE_NAME);
    // Create the table using Hive
    createManagedTable(
        INGESTION_TIME_PARTITIONED_TABLE_NAME,
        HIVE_INGESTION_TIME_PARTITIONED_DDL,
        HIVE_INGESTION_TIME_PARTITIONED_PROPS,
        null);
    runHiveScript(
        String.format(
            "SELECT * from %s WHERE `_PARTITIONTIME` > TIMESTAMP'2018-09-05 00:10:04.19'",
            INGESTION_TIME_PARTITIONED_TABLE_NAME));
    runHiveScript(
        String.format(
            "SELECT * from %s WHERE `_PARTITIONDATE` <= DATE'2019-08-02'",
            INGESTION_TIME_PARTITIONED_TABLE_NAME));
  }

  @Test
  public void readPartitionedTabled() {
    hive.setHiveConfValue(
        HiveBigQueryConfig.WRITE_METHOD_KEY, HiveBigQueryConfig.WRITE_METHOD_DIRECT);

    hive.setHiveConfValue(
        "hive.metastore.rawstore.impl",
        "com.google.cloud.hive.bigquery.connector.BigQueryMetadataStore");

    initHive("mr", HiveBigQueryConfig.ARROW);
    String tableName = "lala";
    runBqQuery(
        String.join(
            "\n",
            "create or replace table `${dataset}." + tableName + "`",
            "(",
            "    int_col int64,",
            "    str_col string,",
            "    float_col float64,",
            "    bool_col boolean,",
            "    date_col date",
            ")",
            "partition by date_col"));
    runBqQuery(
        String.join(
            "\n",
            "insert into `${dataset}." + tableName + "` values",
            "(101, 'banana', 2.2, False, date('2001-01-01')),",
            "(102, 'apple', 2.3, True, date('2001-01-01')),",
            "(103, 'berry', 2.4, False, date('2002-02-02')),",
            "(104, 'watermelon', 2.5, True, date('2002-02-02')),",
            "(105, 'sweetlime', 2.6, False, date('2003-03-03')),",
            "(106, 'papaya', 2.7, True, date('2003-03-03'))"));
    runHiveScript(
        String.join(
            "\n",
            "CREATE EXTERNAL TABLE `" + tableName + "` (",
            "int_col int,",
            "str_col string,",
            "float_col float,",
            "bool_col boolean",
            ")",
            "PARTITIONED BY (date_col date)",
            "STORED BY 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'",
            "TBLPROPERTIES (",
            "    'bq.project'='${project}',",
            "    'bq.dataset'='${dataset}',",
            "    'bq.table'='" + tableName + "'",
            ")"));

    List<Object[]> rows = runHiveStatement("show partitions " + tableName);
//    List<Object[]> rows = runHiveStatement("select * from " + tableName);
//    List<Object[]> rows = runHiveStatement(
//        "select * from " + tableName +
//            " WHERE date_col >= '2003-03-03' and date_col < '2005-05-05'");
    assertTrue(rows.size() > 0);
  }
}
