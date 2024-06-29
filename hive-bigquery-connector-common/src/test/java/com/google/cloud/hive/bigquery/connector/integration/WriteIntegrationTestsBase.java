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
import static com.google.cloud.hive.bigquery.connector.TestUtils.TEST_TABLE_NAME;
import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.hive.bigquery.connector.TestUtils;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.storage.Blob;
import com.google.common.collect.Streams;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public abstract class WriteIntegrationTestsBase extends IntegrationTestsBase {

  /** Insert data into a simple table. */
  public void insert(String engine, String writeMethod, String tempGcsPath) {
    System.getProperties().setProperty(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    if (tempGcsPath != null) {
      initHive(engine, HiveBigQueryConfig.AVRO, tempGcsPath);
    } else {
      initHive(engine, HiveBigQueryConfig.AVRO);
    }
    createExternalTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL, BIGQUERY_TEST_TABLE_DDL);
    // Run two insert queries using Hive
    runHiveQuery("INSERT INTO " + TEST_TABLE_NAME + " VALUES (123, 'hello')");
    runHiveQuery("INSERT INTO " + TEST_TABLE_NAME + " VALUES (789, 'abcd')");
    // Read the data using the BQ SDK
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `${dataset}.%s` ORDER BY number", TEST_TABLE_NAME));
    // Verify we get the expected values
    assertEquals(2, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    assertEquals(123L, rows.get(0).get(0).getLongValue());
    assertEquals("hello", rows.get(0).get(1).getStringValue());
    assertEquals(789L, rows.get(1).get(0).getLongValue());
    assertEquals("abcd", rows.get(1).get(1).getStringValue());
    // Make sure things are correctly cleaned up
    checkThatWorkDirsHaveBeenCleaned();
  }

  /** Insert data using the "direct" write method. */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE)
  public void testInsertDirect(String engine) {
    insert(engine, HiveBigQueryConfig.WRITE_METHOD_DIRECT, null);
  }

  // ---------------------------------------------------------------------------------------------------

  /** Insert data using the "indirect" write method. */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE)
  public void testInsertIndirect(String engine) {
    String testGcsDir =
        this.tempGcsDir + (this.tempGcsDir.endsWith("/") ? "" : "/") + "indirect-test";
    String testGcsPath = "gs://" + testBucketName + "/" + testGcsDir;

    // Check that the dir is empty
    List<Blob> blobs = getBlobs(testBucketName, testGcsDir);
    assertEquals(0, blobs.size(), "Unexpected blobs: " + blobs);

    // Insert data using Hive
    insert(engine, HiveBigQueryConfig.WRITE_METHOD_INDIRECT, testGcsPath);
  }

  // ---------------------------------------------------------------------------------------------------

  /** Test the "INSERT OVERWRITE" statement, which clears the table before writing the new data. */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_WRITE_METHOD)
  public void testInsertOverwrite(String engine, String writeMethod) {
    System.getProperties().setProperty(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    initHive(engine, HiveBigQueryConfig.AVRO);
    createExternalTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL, BIGQUERY_TEST_TABLE_DDL);
    // Create some initial data in BQ
    runBqQuery(
        String.format(
            "INSERT `${dataset}.%s` VALUES (123, 'hello'), (999, 'abcd')", TEST_TABLE_NAME));
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `${dataset}.%s`", TEST_TABLE_NAME));
    // Make sure the initial data is there
    assertEquals(2, result.getTotalRows());
    // Run INSERT OVERWRITE in Hive
    runHiveQuery("INSERT OVERWRITE TABLE " + TEST_TABLE_NAME + " VALUES (888, 'xyz')");
    // Make sure the new data erased the old one
    result = runBqQuery(String.format("SELECT * FROM `${dataset}.%s`", TEST_TABLE_NAME));
    assertEquals(1, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    assertEquals(888L, rows.get(0).get(0).getLongValue());
    assertEquals("xyz", rows.get(0).get(1).getStringValue());
    // Make sure things are correctly cleaned up
    checkThatWorkDirsHaveBeenCleaned();
  }

  // ---------------------------------------------------------------------------------------------------

  /** Test "INSERT TABLE AS SELECT" (ITAS) */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_WRITE_METHOD)
  public void testITAS(String engine, String writeMethod) {
    System.getProperties().setProperty(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    initHive(engine, HiveBigQueryConfig.AVRO);
    String itasSource = "itas_source";
    String itasDestination = "itas_destination";
    createExternalTable(itasSource, HIVE_TEST_TABLE_DDL, BIGQUERY_TEST_TABLE_DDL);
    createExternalTable(itasDestination, HIVE_TEST_TABLE_DDL, BIGQUERY_TEST_TABLE_DDL);
    // Insert data into the BQ table using the BQ SDK
    runBqQuery(
        String.join(
            "\n",
            String.format("INSERT `${dataset}.%s` VALUES", itasSource),
            "(1, 'hello'), (2, 'bonjour')"));
    // Run ITAS query in Hive
    runHiveQuery("INSERT INTO " + itasDestination + " SELECT * FROM " + itasSource);
    // Make sure the data was written to BQ
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `${dataset}.%s` ORDER BY number", itasDestination));
    assertEquals(2, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    assertEquals(1L, rows.get(0).get(0).getLongValue());
    assertEquals("hello", rows.get(0).get(1).getStringValue());
    assertEquals(2L, rows.get(1).get(0).getLongValue());
    assertEquals("bonjour", rows.get(1).get(1).getStringValue());
    // Make sure things are correctly cleaned up
    checkThatWorkDirsHaveBeenCleaned();
  }

  // ---------------------------------------------------------------------------------------------------

  /** Test "INSERT OVERWRITE AS SELECT" */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_WRITE_METHOD)
  public void testOverwriteITAS(String engine, String writeMethod) {
    System.getProperties().setProperty(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    initHive(engine, HiveBigQueryConfig.AVRO);
    String itasOverwriteSource = "itas_overwrite_source";
    String itasOverwriteDestination = "itas_overwrite_destination";
    createExternalTable(itasOverwriteSource, HIVE_TEST_TABLE_DDL, BIGQUERY_TEST_TABLE_DDL);
    createExternalTable(itasOverwriteDestination, HIVE_TEST_TABLE_DDL, BIGQUERY_TEST_TABLE_DDL);
    // Insert data into the BQ tables using the BQ SDK
    runBqQuery(
        String.join(
            "\n",
            String.format("INSERT `${dataset}.%s` VALUES", itasOverwriteSource),
            "(1, 'hello'), (2, 'bonjour')"));
    runBqQuery(
        String.join(
            "\n",
            String.format("INSERT `${dataset}.%s` VALUES", itasOverwriteDestination),
            "(1, 'hola'), (2, 'gutentag')"));
    // Run ITAS query in Hive
    runHiveQuery(
        "INSERT OVERWRITE TABLE "
            + itasOverwriteDestination
            + " SELECT * FROM "
            + itasOverwriteSource);
    // Make sure the data was written to BQ
    TableResult result =
        runBqQuery(
            String.format(
                "SELECT * FROM `${dataset}.%s` ORDER BY number", itasOverwriteDestination));
    assertEquals(2, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    assertEquals(1L, rows.get(0).get(0).getLongValue());
    assertEquals("hello", rows.get(0).get(1).getStringValue());
    assertEquals(2L, rows.get(1).get(0).getLongValue());
    assertEquals("bonjour", rows.get(1).get(1).getStringValue());
    // Make sure things are correctly cleaned up
    checkThatWorkDirsHaveBeenCleaned();
  }

  // ---------------------------------------------------------------------------------------------------

  /** Test the multi insert statement, which inserts into multiple tables. */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_WRITE_METHOD)
  public void testMultiInsert(String engine, String writeMethod) {
    // TODO: Figure out why vectorization and map-joins must be disabled for this test to pass
    System.getProperties().setProperty(HiveConf.ConfVars.HIVECONVERTJOIN.varname, "false");
    System.getProperties()
        .setProperty(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED.varname, "false");
    System.getProperties().setProperty(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    initHive(engine, HiveBigQueryConfig.AVRO);
    createExternalTable("bq_a", "id int, name string", "id int64, name string");
    createExternalTable("bq_b", "id int, name string", "id int64, name string");
    String setupQ =
        "create table hdfs_a (id int, name string); "
            + "create table hdfs_b (id int, name string); "
            + "insert into table hdfs_a values (1, 'aaa'), (2, 'bbb'); "
            + "insert into table hdfs_b values (1, 'aaa'), (2, 'ccc'); ";
    String multiInsertQ =
        "with c as (select a.id, b.name from hdfs_a a join hdfs_b b on a.id=b.id and a.name=b.name"
            + " ) from c insert overwrite table bq_a select id*10, name insert into table bq_b"
            + " select count(1)*20, name group by name;";
    runHiveScript(setupQ);
    runHiveScript(multiInsertQ);

    TableResult resultA = runBqQuery("SELECT * FROM `${dataset}.bq_a`");
    assertEquals(1, resultA.getTotalRows());
    List<FieldValueList> rows = Streams.stream(resultA.iterateAll()).collect(Collectors.toList());
    assertEquals(10L, rows.get(0).get(0).getLongValue());
    assertEquals("aaa", rows.get(0).get(1).getStringValue());

    TableResult resultB = runBqQuery("SELECT * FROM `${dataset}.bq_b`");
    assertEquals(1, resultB.getTotalRows());
    rows = Streams.stream(resultB.iterateAll()).collect(Collectors.toList());
    assertEquals(20L, rows.get(0).get(0).getLongValue());
    assertEquals("aaa", rows.get(0).get(1).getStringValue());
    // Make sure things are correctly cleaned up
    checkThatWorkDirsHaveBeenCleaned();
  }

  // ---------------------------------------------------------------------------------------------------

  /** Check that we can write all types of data to BigQuery. */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_WRITE_METHOD)
  public void testWriteAllTypes(String engine, String writeMethod) {
    System.getProperties().setProperty(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    initHive(engine, HiveBigQueryConfig.AVRO);
    // Create the BQ table
    createExternalTable(
        ALL_TYPES_TABLE_NAME, HIVE_ALL_TYPES_TABLE_DDL, BIGQUERY_ALL_TYPES_TABLE_DDL);
    // Insert data into the BQ table using Hive
    runHiveQuery(
        String.join(
            "\n",
            "INSERT INTO " + ALL_TYPES_TABLE_NAME + " SELECT",
            "11,",
            "22,",
            "33,",
            "44,",
            "true,",
            "\"fixed char\",",
            "\"var char\",",
            "\"string\",",
            "CAST(\"2019-03-18\" AS DATE),",
            // Wall clock (no timezone)
            "CAST(\"2000-01-01 00:23:45.123456\" as TIMESTAMP),",
            "CAST(\"bytes\" AS BINARY),",
            "2.0,",
            "4.2,",
            "NAMED_STRUCT(",
            "  'min', CAST('-99999999999999999999999999999.999999999' AS" + " DECIMAL(38,9)),",
            "  'max', CAST('99999999999999999999999999999.999999999' AS" + " DECIMAL(38,9)),",
            "  'pi', CAST('3.14' AS DECIMAL(38,9)),",
            "  'big_pi', CAST('31415926535897932384626433832.795028841' AS" + " DECIMAL(38,9))",
            "),",
            "ARRAY(CAST (1 AS BIGINT), CAST (2 AS BIGINT), CAST (3 AS" + " BIGINT)),",
            "ARRAY(NAMED_STRUCT('i', CAST (1 AS BIGINT))),",
            "NAMED_STRUCT('float_field', CAST(4.2 AS FLOAT), 'ts_field', CAST"
                + " (\"2019-03-18 01:23:45.678901\" AS TIMESTAMP)),",
            "MAP('mykey', MAP('subkey', 999))",
            "FROM (select '1') t"));
    // Read the data using the BQ SDK
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `${dataset}.%s`", ALL_TYPES_TABLE_NAME));
    // Verify we get the expected values
    assertEquals(1, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    FieldValueList row = rows.get(0);
    assertEquals(18, row.size()); // Number of columns
    assertEquals(11L, row.get(0).getLongValue());
    assertEquals(22L, row.get(1).getLongValue());
    assertEquals(33L, row.get(2).getLongValue());
    assertEquals(44L, row.get(3).getLongValue());
    assertTrue(row.get(4).getBooleanValue());
    assertEquals("fixed char", row.get(5).getStringValue());
    assertEquals("var char", row.get(6).getStringValue());
    assertEquals("string", row.get(7).getStringValue());
    assertEquals("2019-03-18", row.get(8).getStringValue());
    assertEquals("2000-01-01T00:23:45.123456", row.get(9).getStringValue());
    assertArrayEquals("bytes".getBytes(), row.get(10).getBytesValue());
    assertEquals(2.0, row.get(11).getDoubleValue());
    assertEquals(4.2, row.get(12).getDoubleValue());
    FieldValueList struct = row.get(13).getRecordValue();
    assertEquals(
        "-99999999999999999999999999999.999999999",
        struct.get("min").getNumericValue().toPlainString());
    assertEquals(
        "99999999999999999999999999999.999999999",
        struct.get("max").getNumericValue().toPlainString());
    assertEquals("3.14", struct.get("pi").getNumericValue().toPlainString());
    assertEquals(
        "31415926535897932384626433832.795028841",
        struct.get("big_pi").getNumericValue().toPlainString());
    FieldValueList array = (FieldValueList) row.get(14).getValue();
    assertEquals(3, array.size());
    assertEquals(1, array.get(0).getLongValue());
    assertEquals(2, array.get(1).getLongValue());
    assertEquals(3, array.get(2).getLongValue());
    FieldValueList arrayOfStructs = (FieldValueList) row.get(15).getValue();
    assertEquals(1, arrayOfStructs.size());
    struct = (FieldValueList) arrayOfStructs.get(0).getValue();
    assertEquals(1L, struct.get(0).getLongValue());
    // Mixed struct
    struct = row.get(16).getRecordValue();
    assertEquals(
        4.199999809265137,
        struct.get("float_field").getDoubleValue()); // TODO: Address discrepancy here
    assertEquals("2019-03-18T01:23:45.678901", struct.get("ts_field").getStringValue());
    // Check the Map type
    FieldValueList map = (FieldValueList) row.get(17).getRepeatedValue();
    assertEquals(1, map.size());
    FieldValueList entry = map.get(0).getRecordValue();
    assertEquals("mykey", entry.get(0).getStringValue());
    assertEquals(1, entry.get(1).getRepeatedValue().size());
    FieldValueList subEntry = entry.get(1).getRepeatedValue().get(0).getRecordValue();
    assertEquals("subkey", subEntry.get(0).getStringValue());
    assertEquals(999, subEntry.get(1).getLongValue());
    // Make sure things are correctly cleaned up
    checkThatWorkDirsHaveBeenCleaned();
  }

  // ---------------------------------------------------------------------------------------------------

  /** Check that we can write NULL values in all types of fields. */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_WRITE_METHOD)
  public void testWriteAllTypesNull(String engine, String writeMethod) {
    System.getProperties().setProperty(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    initHive(engine, HiveBigQueryConfig.AVRO);
    // Create the BQ table
    createExternalTable(
        ALL_TYPES_TABLE_NAME, HIVE_ALL_TYPES_TABLE_DDL, BIGQUERY_ALL_TYPES_TABLE_DDL);
    // Insert data into the BQ table using Hive
    runHiveQuery(
        String.join(
            "\n",
            "INSERT INTO " + ALL_TYPES_TABLE_NAME + " SELECT",
            "NULL,",
            "NULL,",
            "NULL,",
            "NULL,",
            "NULL,",
            "NULL,",
            "NULL,",
            "NULL,",
            "NULL,",
            "NULL,",
            "NULL,",
            "NULL,",
            "NULL,",
            // Note: Hive doesn't allow just passing "NULL" as a value for complex types,
            // so we use a workaround using "IF(FALSE, ..., NULL)" as suggested here:
            // https://issues.apache.org/jira/browse/HIVE-4022?focusedCommentId=14268625#comment-14268625
            "IF(FALSE, NAMED_STRUCT(",
            "  'min', CAST(0.0 AS" + " DECIMAL(38,9)),",
            "  'max', CAST(0.0 AS" + " DECIMAL(38,9)),",
            "  'pi', CAST(0.0 AS DECIMAL(38,9)),",
            "  'big_pi', CAST(0.0 AS" + " DECIMAL(38,9))",
            "), NULL),",
            "IF(FALSE, ARRAY(CAST (1 AS BIGINT)), NULL),",
            "IF(FALSE, ARRAY(NAMED_STRUCT('i', CAST (1 AS BIGINT))), NULL),",
            "IF(FALSE, NAMED_STRUCT('float_field', CAST(0.0 AS FLOAT), 'ts_field', CAST ('' AS TIMESTAMP)), NULL),",
            "IF(FALSE, MAP('mykey', MAP('subkey', 0)), NULL)",
            // Note: In old versions of Hive you need to use a dummy table here.
            // See: https://issues.apache.org/jira/browse/HIVE-12200
            "from (select 'a') dummy"));
    // Read the data using the BQ SDK
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `${dataset}.%s`", ALL_TYPES_TABLE_NAME));
    // Verify we get the expected values
    assertEquals(1, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    FieldValueList row = rows.get(0);
    assertEquals(18, row.size()); // Number of columns
    for (int i = 0; i < 14; i++) {
      assertNull(row.get(i).getValue());
    }
    assertEquals(0, ((FieldValueList) row.get(14).getValue()).size());
    assertEquals(0, ((FieldValueList) row.get(15).getValue()).size());
    assertNull(row.get(16).getValue());
    assertEquals(0, ((FieldValueList) row.get(17).getValue()).size());
    // Make sure things are correctly cleaned up
    checkThatWorkDirsHaveBeenCleaned();
  }

  // ---------------------------------------------------------------------------------------------------

  /** Check that we can write into not-null fields. */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_WRITE_METHOD)
  public void testWriteRequiredFields(String engine, String writeMethod) {
    System.getProperties().setProperty(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    initHive(engine, HiveBigQueryConfig.AVRO);
    String bq_table_ddl =
        String.join(
            "\n",
            "id INT64 NOT NULL,",
            "name STRING NOT NULL,",
            "mixed_struct STRUCT<type STRING NOT NULL, info STRUCT<units INT64 NOT NULL, label STRING NOT NULL>>,",
            "mp ARRAY<STRUCT<key STRING NOT NULL, value ARRAY<STRUCT<key STRING NOT NULL, value INT64 NOT NULL>>>>");
    String hive_table_ddl =
        String.join(
            "\n",
            "id INT,",
            "name STRING,",
            "mixed_struct STRUCT<type: STRING, info: STRUCT<units: INT, label: STRING>>,",
            "mp MAP<STRING, MAP<STRING,INT>>");
    createExternalTable("test_required", hive_table_ddl, bq_table_ddl);
    runHiveQuery(
        String.join(
            "\n",
            "INSERT INTO test_required SELECT",
            "1,",
            "'james',",
            "NAMED_STRUCT('type', 'apartment', 'info', NAMED_STRUCT('units', 2, 'label', 'medium')),",
            "MAP('mykey', MAP('subkey', 0))",
            // Note: In old versions of Hive you need to use a dummy table here.
            // See: https://issues.apache.org/jira/browse/HIVE-12200
            "from (select 'a') dummy"));
    // Read the data using the BQ SDK
    TableResult result = runBqQuery("SELECT * FROM `${dataset}.test_required`");
    // Verify we get the expected values
    assertEquals(1, result.getTotalRows());
    // Make sure things are correctly cleaned up
    checkThatWorkDirsHaveBeenCleaned();
  }

  // ---------------------------------------------------------------------------------------------------

  /** Check that we can write all types of data to BigQuery. */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_WRITE_METHOD)
  public void testWriteDecimals(String engine, String writeMethod) {
    // To-Do: fix avro when upgrade bigquery-connector-common
    System.getProperties().setProperty(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    initHive(engine, HiveBigQueryConfig.AVRO);
    // Create the BQ table
    createExternalTable(
        "mixedDecimals",
        "deci_default DECIMAL,  deci_p38s9_min DECIMAL(38,9), deci_p38s9_max DECIMAL(38,9), deci_p38s8 DECIMAL(38,8), deci_p38s10 DECIMAL(38,10), deci_p38s38_min DECIMAL(38,38), deci_p38s38_max DECIMAL(38,38)",
        "deci_default NUMERIC(10,0), deci_p38s9_min NUMERIC, deci_p38s9_max NUMERIC, deci_p38s8 BIGNUMERIC(38,8), deci_p38s10 BIGNUMERIC(38,10), deci_p38s38_min BIGNUMERIC(38,38), deci_p38s38_max BIGNUMERIC(38,38)");
    // Insert data into the BQ table using the BQ SDK
    String[] values = {
      "9999999999", // p10s0
      "-99999999999999999999999999999.999999999", // p38s9_min
      "99999999999999999999999999999.999999999", // p38s9_max
      "999999999999999999999999999999.99999999", // p38s8
      "9999999999999999999999999999.9999999999", // p38s10
      "-0.99999999999999999999999999999999999999", // p38s38_min
      "0.99999999999999999999999999999999999999", // p38s38_max
    };
    runHiveQuery("INSERT INTO `mixedDecimals` VALUES ( " + String.join(",", values) + ")");
    // Read the data using the BQ SDK
    TableResult result = runBqQuery(String.format("SELECT * FROM `${dataset}.mixedDecimals`"));
    // Verify we get the expected values
    assertEquals(1, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    FieldValueList row = rows.get(0);
    assertEquals(7, row.size()); // Number of columns
    for (int i = 0; i <= 6; i++) {
      assertEquals(values[i], row.get(i).getValue().toString());
    }
    // Make sure things are correctly cleaned up
    checkThatWorkDirsHaveBeenCleaned();
  }

  // ---------------------------------------------------------------------------------------------------

  /** Test a write operation and multiple read operations in the same query. */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_READ_FORMAT_WRITE_METHOD)
  public void testMultiReadWrite(String engine, String readDataFormat, String writeMethod) {
    System.getProperties().setProperty(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    initHive(engine, readDataFormat);
    createExternalTable(
        ALL_TYPES_TABLE_NAME, HIVE_ALL_TYPES_TABLE_DDL, BIGQUERY_ALL_TYPES_TABLE_DDL);
    createExternalTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL, BIGQUERY_TEST_TABLE_DDL);
    createExternalTable(
        ANOTHER_TEST_TABLE_NAME, HIVE_ANOTHER_TEST_TABLE_DDL, BIGQUERY_ANOTHER_TEST_TABLE_DDL);
    // Insert data into the BQ tables using the BQ SDK
    runBqQuery(
        String.join(
            "\n",
            String.format("INSERT `${dataset}.%s` (int_val) VALUES", ALL_TYPES_TABLE_NAME),
            "(123), (42), (999)"));
    runBqQuery(
        String.join(
            "\n",
            String.format("INSERT `${dataset}.%s` (str_val) VALUES", ANOTHER_TEST_TABLE_NAME),
            "(\"hello1\"), (\"hello2\"), (\"hello3\")"));
    // Read and write in the same query using Hive
    runHiveQuery(
        String.join(
            "\n",
            "INSERT INTO " + TEST_TABLE_NAME + " SELECT",
            "MAX(number), MAX(text)",
            "FROM (",
            "SELECT",
            "0 as number,",
            "t1.str_val as text",
            "FROM " + ANOTHER_TEST_TABLE_NAME + " t1",
            "UNION ALL",
            "SELECT",
            "t2.int_val as number,",
            "'' as text",
            "FROM " + ALL_TYPES_TABLE_NAME + " t2",
            ") unioned"));
    // Read the result using the BQ SDK
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `${dataset}.%s`", TEST_TABLE_NAME));
    // Verify we get the expected values
    assertEquals(1, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    assertEquals(999, rows.get(0).get(0).getLongValue());
    assertEquals("hello3", rows.get(0).get(1).getStringValue());
    // Make sure things are correctly cleaned up
    checkThatWorkDirsHaveBeenCleaned();
  }

  // ---------------------------------------------------------------------------------------------------

  /** Check that we correctly clean things up in case of a query failure. */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_WRITE_METHOD)
  public void testFailure(String engine, String writeMethod) {
    // Set up the job for failure
    System.getProperties().setProperty(HiveBigQueryConfig.FORCE_COMMIT_FAILURE, "true");
    System.getProperties().setProperty(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    initHive(engine);
    String tableName = String.format("failure_%s", writeMethod);
    createExternalTable(tableName, HIVE_TEST_TABLE_DDL, BIGQUERY_TEST_TABLE_DDL);
    // Make sure the insert query fails
    Throwable exception =
        assertThrows(
            RuntimeException.class,
            () -> runHiveQuery("INSERT INTO " + tableName + " VALUES (123, 'hello')"));
    // Check for error messages, which are only available when using Tez
    if (hive.getHiveConf().getVar(ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) {
      assertTrue(
          exception.getMessage().contains(HiveBigQueryConfig.FORCED_COMMIT_FAILURE_ERROR_MESSAGE));
    }
    // Make sure no rows were inserted
    TableResult result = runBqQuery(String.format("SELECT * FROM `${dataset}.%s`", tableName));
    assertEquals(0, result.getTotalRows());

    // Make sure things are correctly cleaned up
    checkThatWorkDirsHaveBeenCleaned();
  }

  // ---------------------------------------------------------------------------------------------------

  /** Check that we can write to an encrypted table. */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_WRITE_METHOD)
  public void testWriteToCmekTable(String engine, String writeMethod) {
    System.getProperties()
        .setProperty(HiveBigQueryConfig.DESTINATION_TABLE_KMS_KEY_NAME, TestUtils.getKmsKeyName());
    System.getProperties().setProperty(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    initHive(engine);
    String tableName = String.format("cmek_%s", writeMethod);
    createExternalTable(tableName, HIVE_TEST_TABLE_DDL);
    // Run an insert query using Hive
    runHiveQuery("INSERT INTO " + tableName + " VALUES (123, 'hello')");
    // Check that the table was created in BQ with the correct key name
    TableInfo tableInfo = getTableInfo(dataset, tableName);
    assertEquals(TestUtils.getKmsKeyName(), tableInfo.getEncryptionConfiguration().getKmsKeyName());
    // Read the data using the BQ SDK
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `${dataset}.%s` ORDER BY number", tableName));
    // Verify we get the expected values
    assertEquals(1, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    assertEquals(123L, rows.get(0).get(0).getLongValue());
    assertEquals("hello", rows.get(0).get(1).getStringValue());
    // Make sure things are correctly cleaned up
    checkThatWorkDirsHaveBeenCleaned();
  }
}
