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

import com.google.cloud.bigquery.Clustering;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TimePartitioning;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import org.junit.jupiter.api.Test;
import repackaged.by.hivebqconnector.com.google.common.collect.ImmutableList;

public class PartitionIntegrationTests extends IntegrationTestsBase {

  @Test
  public void testFieldTimePartition() {
    // Make sure the BQ table doesn't exist
    dropBqTableIfExists(dataset, FIELD_TIME_PARTITIONED_TABLE_NAME);
    // Create the table using Hive
    initHive();
    runHiveScript(HIVE_FIELD_TIME_PARTITIONED_TABLE_CREATE_QUERY);
    StandardTableDefinition tableDef =
        getTableInfo(dataset, FIELD_TIME_PARTITIONED_TABLE_NAME).getDefinition();
    TimePartitioning timePartitioning = tableDef.getTimePartitioning();
    assertEquals(2592000000L, Objects.requireNonNull(timePartitioning).getExpirationMs());
    assertEquals("ts", timePartitioning.getField());
    assertEquals(TimePartitioning.Type.MONTH, timePartitioning.getType());
    Clustering clustering = tableDef.getClustering();
    assertEquals(ImmutableList.of("int_val"), Objects.requireNonNull(clustering).getFields());
  }

  @Test
  public void testPartitionClause() {
    // Make sure the BQ table doesn't exist
    dropBqTableIfExists(dataset, PARTITION_CLAUSE_TABLE_NAME);
    // Create the table using Hive
    initHive();
    runHiveScript(HIVE_PARTITION_CLAUSE_TABLE_CREATE_QUERY);
    StandardTableDefinition tableDef =
        getTableInfo(dataset, PARTITION_CLAUSE_TABLE_NAME).getDefinition();
    TimePartitioning timePartitioning = tableDef.getTimePartitioning();
    assertEquals(2592000000L, Objects.requireNonNull(timePartitioning).getExpirationMs());
    assertEquals("ts", timePartitioning.getField());
    assertEquals(TimePartitioning.Type.MONTH, timePartitioning.getType());
    Clustering clustering = tableDef.getClustering();
    assertEquals(ImmutableList.of("int_val"), Objects.requireNonNull(clustering).getFields());
  }


  @Test
  public void testPartitionByClauseDate() {
//    initHive("mr", HiveBigQueryConfig.ARROW);
    initHive();
    String tableName = "lala";
    String query = String.join("\n",
        "CREATE TABLE " + tableName + " (",
        "int_val BIGINT, bobo TIMESTAMP WITH LOCAL TIME ZONE",
        ")",
        "PARTITIONED BY (dt DATE)",
        "STORED BY" + " 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'",
        "TBLPROPERTIES (",
        "  'bq.project'='${project}',",
        "  'bq.dataset'='${dataset}',",
        "  'bq.table'='" + tableName + "'",
        ");");
    runHiveScript(query);
    query = String.join("\n",
        "INSERT INTO TABLE " + tableName + " PARTITION(dt='2022-12-01') VALUES(",
        "999",
        ");");

//    query = String.join("\n",
//        "INSERT INTO TABLE " + tableName + " VALUES(",
//        "999, '2022-12-01'",
//        ");");

//    query = Stream.of(
//            "INSERT INTO TABLE " + tableName + " VALUES(",
//              "999, '2022-12-01'",
//            ");")
//        .collect(Collectors.joining("\n"));
    runHiveScript(query);
    int a = 1;
  }


  @Test
  public void testInsertOverwriteWithPartition() {
    initHive();
    // Make sure the BQ table doesn't exist
    dropBqTableIfExists(dataset, INGESTION_TIME_PARTITIONED_TABLE_NAME);
    // Create the table using Hive
    runHiveScript(HIVE_INGESTION_TIME_PARTITIONED_TABLE_CREATE_QUERY);
    runHiveScript(
        String.format(
            "INSERT OVERWRITE TABLE %s\n" +
                "PARTITION(order_date='2018-08-01') VALUES \n" +
                "(999);\n",
            INGESTION_TIME_PARTITIONED_TABLE_NAME));
  }

  @Test
  public void testCreateIngestionTimePartition() {
    // Make sure the BQ table doesn't exist
    dropBqTableIfExists(dataset, INGESTION_TIME_PARTITIONED_TABLE_NAME);
    // Create the table using Hive
    initHive();
    runHiveScript(HIVE_INGESTION_TIME_PARTITIONED_TABLE_CREATE_QUERY);
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
    runHiveScript(HIVE_INGESTION_TIME_PARTITIONED_TABLE_CREATE_QUERY);
    runHiveScript(
        String.format(
            "SELECT * from %s WHERE `_PARTITIONTIME` > TIMESTAMP'2018-09-05 00:10:04.19'",
            INGESTION_TIME_PARTITIONED_TABLE_NAME));
    runHiveScript(
        String.format(
            "SELECT * from %s WHERE `_PARTITIONDATE` <= DATE'2019-08-02'",
            INGESTION_TIME_PARTITIONED_TABLE_NAME));
  }

}
