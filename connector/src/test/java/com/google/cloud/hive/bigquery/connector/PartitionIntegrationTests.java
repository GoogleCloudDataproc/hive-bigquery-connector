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
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import repackaged.by.hivebqconnector.com.google.common.collect.ImmutableList;
import repackaged.by.hivebqconnector.com.google.common.collect.Streams;

public class PartitionIntegrationTests extends IntegrationTestsBase {

  // TODO: Test insert with dynamic partitioning
  //  (hive.exec.dynamic.partition=true, hive.exec.dynamic.partition.mode=nonstrict)

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

    // Insert using "INTO TABLE PARTITION(...) VALUES" statement
    runHiveScript(
        String.join(
            "\n",
            "INSERT INTO TABLE "
                + PARTITION_CLAUSE_TABLE_NAME
                + " PARTITION(dt='2022-12-01') VALUES(",
            "888",
            ");"));
    // Insert using "INTO TABLE VALUES" statement (without "PARTITION(...)")
    runHiveScript(
        String.join(
            "\n",
            "INSERT INTO TABLE " + PARTITION_CLAUSE_TABLE_NAME + " VALUES(",
            "999, '2023-01-03'",
            ");"));
    // Insert using "INTO TABLE SELECT" statement
    runHiveScript(
        String.join(
            "\n",
            "INSERT INTO TABLE "
                + PARTITION_CLAUSE_TABLE_NAME
                + " PARTITION(dt='2018-04-07') SELECT(",
            "1000",
            ");"));

    // Read the data using the BQ SDK
    TableResult result =
        runBqQuery(
            String.format(
                "SELECT * FROM `${dataset}.%s` ORDER BY int_val", PARTITION_CLAUSE_TABLE_NAME));

    // Verify we get the expected values
    assertEquals(3, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    assertEquals(888L, rows.get(0).get(0).getLongValue());
    assertEquals("2022-12-01", rows.get(0).get(1).getStringValue());
    assertEquals(999L, rows.get(1).get(0).getLongValue());
    assertEquals("2023-01-03", rows.get(1).get(1).getStringValue());
    assertEquals(1000L, rows.get(2).get(0).getLongValue());
    assertEquals("2018-04-07", rows.get(2).get(1).getStringValue());
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
    hive.setHiveConfValue(
        HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL.varname,
        "com.google.cloud.hive.bigquery.connector.metastore.BigQueryObjectStore");
    hive.setHiveConfValue(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    initHive(engine, HiveBigQueryConfig.ARROW);
    // Make sure the BQ table doesn't exist
    dropBqTableIfExists(dataset, PARTITION_CLAUSE_TABLE_NAME);
    // Create the table using Hive
    runHiveScript(HIVE_PARTITION_CLAUSE_TABLE_CREATE_QUERY);
    // Add some initial data
    runBqQuery(
        String.format(
            "INSERT `${dataset}.%s` VALUES (123, '2019-08-15'), (555, '2018-07-12'), (888,"
                + " '2022-12-01'), (999, '2022-12-01')",
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
    // Overwrite a partition from Hive using VALUES
    runHiveScript(
        String.join(
            "\n",
            "INSERT OVERWRITE TABLE "
                + PARTITION_CLAUSE_TABLE_NAME
                + " PARTITION(dt='2022-12-01') VALUES(",
            "777",
            ");"));
    // Overwrite another partition from Hive using SELECT
    runHiveScript(
        String.join(
            "\n",
            "INSERT OVERWRITE TABLE "
                + PARTITION_CLAUSE_TABLE_NAME
                + " PARTITION(dt='2018-07-12') SELECT(",
            "222",
            ");"));
    TableResult result =
        runBqQuery(
            String.format(
                "SELECT * FROM `${dataset}.%s` ORDER BY int_val", PARTITION_CLAUSE_TABLE_NAME));
    // Verify we get the expected values
    assertEquals(3, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    assertEquals(123L, rows.get(0).get(0).getLongValue());
    assertEquals("2019-08-15", rows.get(0).get(1).getStringValue());
    assertEquals(222L, rows.get(1).get(0).getLongValue());
    assertEquals("2018-07-12", rows.get(1).get(1).getStringValue());
    assertEquals(777L, rows.get(2).get(0).getLongValue());
    assertEquals("2022-12-01", rows.get(2).get(1).getStringValue());
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

  public void createDatePartitionedTable(String engine) {
    hive.setHiveConfValue(
        HiveBigQueryConfig.WRITE_METHOD_KEY, HiveBigQueryConfig.WRITE_METHOD_DIRECT);
    hive.setHiveConfValue(
        HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL.varname,
        "com.google.cloud.hive.bigquery.connector.metastore.BigQueryObjectStore");
    initHive(engine, HiveBigQueryConfig.ARROW);
    String tableName = "partitioned_by_date";
    runBqQuery(
        String.join(
            "\n",
            "create or replace table `${dataset}." + tableName + "`",
            "(",
            "    int_col int64,",
            "    str_col string,",
            "    date_col date",
            ")",
            "partition by date_col"));
    runBqQuery(
        String.join(
            "\n",
            "insert into `${dataset}." + tableName + "` values",
            "(101, 'banana', date('2001-01-01')),",
            "(102, 'apple', date('2001-01-01')),",
            "(103, 'berry', date('2002-02-02')),",
            "(104, 'watermelon', date('2002-02-02')),",
            "(105, 'sweetlime', date('2003-03-03')),",
            "(106, 'papaya', date('2003-03-03'))"));
    runHiveScript(
        String.join(
            "\n",
            "CREATE EXTERNAL TABLE `" + tableName + "` (",
            "int_col int,",
            "str_col string",
            ")",
            "PARTITIONED BY (date_col date)",
            "STORED BY 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'",
            "TBLPROPERTIES (",
            "    'bq.project'='${project}',",
            "    'bq.dataset'='${dataset}',",
            "    'bq.table'='" + tableName + "'",
            ")"));
  }

  @CartesianTest
  public void showPartitions(@CartesianTest.Values(strings = {"mr", "tez"}) String engine) {
    createDatePartitionedTable(engine);
    List<Object[]> rows = runHiveStatement("SHOW PARTITIONS partitioned_by_date");
    rows.sort(Comparator.comparing(x -> (String) x[0]));
    assertArrayEquals(
        new Object[] {
          new Object[] {"date_col=2001-01-01"},
          new Object[] {"date_col=2002-02-02"},
          new Object[] {"date_col=2003-03-03"}
        },
        rows.toArray());
    rows = runHiveStatement("SHOW PARTITIONS partitioned_by_date PARTITION(date_col='2003-03-03')");
    assertArrayEquals(new Object[] {new Object[] {"date_col=2003-03-03"}}, rows.toArray());
    rows = runHiveStatement("SHOW PARTITIONS partitioned_by_date PARTITION(date_col='2000-01-01')");
    assertEquals(0, rows.size());
  }

  @CartesianTest
  public void readDatePartitionedTable(
      @CartesianTest.Values(strings = {"mr", "tez"}) String engine) {
    createDatePartitionedTable(engine);
    List<Object[]> rows = runHiveStatement("SELECT * FROM partitioned_by_date ORDER BY int_col");
    assertArrayEquals(
        new Object[] {
          new Object[] {101, "banana", "2001-01-01"},
          new Object[] {102, "apple", "2001-01-01"},
          new Object[] {103, "berry", "2002-02-02"},
          new Object[] {104, "watermelon", "2002-02-02"},
          new Object[] {105, "sweetlime", "2003-03-03"},
          new Object[] {106, "papaya", "2003-03-03"}
        },
        rows.toArray());
    rows =
        runHiveStatement(
            "SELECT * FROM partitioned_by_date WHERE date_col >= '2002-01-03' AND date_col <"
                + " '2003-02-02' ORDER BY int_col");
    assertArrayEquals(
        new Object[] {
          new Object[] {103, "berry", "2002-02-02"},
          new Object[] {104, "watermelon", "2002-02-02"},
        },
        rows.toArray());
  }
}
