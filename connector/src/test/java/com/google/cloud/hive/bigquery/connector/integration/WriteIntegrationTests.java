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
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.storage.Blob;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import com.google.common.collect.Streams;

public class WriteIntegrationTests extends IntegrationTestsBase {

  // ---------------------------------------------------------------------------------------------------

  /** Insert data into a simple table. */
  public void insert(String engine, String writeMethod, String tempGcsPath) {
    hive.setHiveConfValue(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    if (tempGcsPath != null) {
      initHive(engine, HiveBigQueryConfig.AVRO, tempGcsPath);
    } else {
      initHive(engine, HiveBigQueryConfig.AVRO);
    }
    createExternalTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL, BIGQUERY_TEST_TABLE_DDL);
    // Insert data using Hive
    runHiveQuery("INSERT INTO " + TEST_TABLE_NAME + " VALUES (123, 'hello')");
    // Read the data using the BQ SDK
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `${dataset}.%s`", TEST_TABLE_NAME));
    // Verify we get the expected values
    assertEquals(1, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    assertEquals(123L, rows.get(0).get(0).getLongValue());
    assertEquals("hello", rows.get(0).get(1).getStringValue());
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

    // Check that the blob was created by the job.
    // Note: The blobs are still present here during the test execution because the
    // Hive/Hadoop session is still on, but in production those files would be
    // automatically be cleaned up at the end of the job.
    blobs = getBlobs(testBucketName, testGcsDir);
    assertEquals(1, blobs.size(), "Actual blobs: " + blobs);
    String blobName = blobs.get(0).getName();
    assertTrue(
        blobName.startsWith(testGcsDir) && blobName.endsWith(".avro"),
        "Unexpected blob name: " + blobName);
  }

  // ---------------------------------------------------------------------------------------------------

  /** Test the "INSERT OVERWRITE" statement, which clears the table before writing the new data. */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_WRITE_METHOD)
  public void testInsertOverwrite(String engine, String writeMethod) {
    hive.setHiveConfValue(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
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
  }

  // ---------------------------------------------------------------------------------------------------

  /** Test "INSERT TABLE AS SELECT" (ITAS) */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_WRITE_METHOD)
  public void testITAS(String engine, String writeMethod) {
    hive.setHiveConfValue(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
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
  }

  // ---------------------------------------------------------------------------------------------------

  /** Test "INSERT OVERWRITE AS SELECT" */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_WRITE_METHOD)
  public void testOverwriteITAS(String engine, String writeMethod) {
    hive.setHiveConfValue(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
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
  }

  // ---------------------------------------------------------------------------------------------------

  /** Test the multi insert statement, which inserts into multiple tables. */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_WRITE_METHOD)
  public void testMultiInsert(String engine, String writeMethod) {
    // TODO: Figure out why vectorization and map-joins must be disabled for this test to pass
    hive.setHiveConfValue(HiveConf.ConfVars.HIVECONVERTJOIN.varname, "false");
    hive.setHiveConfValue(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED.varname, "false");
    hive.setHiveConfValue(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
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
  }

  // ---------------------------------------------------------------------------------------------------

  /** Check that we can write all types of data to BigQuery. */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_WRITE_METHOD)
  public void testWriteAllTypes(String engine, String writeMethod) {
    hive.setHiveConfValue(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
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
            "CAST(\"2000-01-01T00:23:45.123456\" as TIMESTAMP),",
            // (Pacific/Honolulu, -10:00)
            "CAST(\"2000-01-01 00:23:45.123456 Pacific/Honolulu\" AS TIMESTAMPLOCALTZ),",
            "CAST(\"bytes\" AS BINARY),",
            "2.0,",
            "4.2,",
            "NAMED_STRUCT(",
            "  'min', CAST(-99999999999999999999999999999.999999999 AS" + " DECIMAL(38,9)),",
            "  'max', CAST(99999999999999999999999999999.999999999 AS" + " DECIMAL(38,9)),",
            "  'pi', CAST(3.14 AS DECIMAL(38,9)),",
            "  'big_pi', CAST(31415926535897932384626433832.795028841 AS" + " DECIMAL(38,9))",
            "),",
            "ARRAY(CAST (1 AS BIGINT), CAST (2 AS BIGINT), CAST (3 AS" + " BIGINT)),",
            "ARRAY(NAMED_STRUCT('i', CAST (1 AS BIGINT))),",
            "NAMED_STRUCT('float_field', CAST(4.2 AS FLOAT), 'ts_field', CAST"
                + " (\"2019-03-18T01:23:45.678901\" AS TIMESTAMP)),",
            "MAP('mykey', MAP('subkey', 999))",
            "FROM (select '1') t"));
    // Read the data using the BQ SDK
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `${dataset}.%s`", ALL_TYPES_TABLE_NAME));
    // Verify we get the expected values
    assertEquals(1, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    FieldValueList row = rows.get(0);
    assertEquals(19, row.size()); // Number of columns
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
    assertEquals(
        "2000-01-01T10:23:45.123456Z", row.get(10).getTimestampInstant().toString()); // 'Z' == UTC
    assertArrayEquals("bytes".getBytes(), row.get(11).getBytesValue());
    assertEquals(2.0, row.get(12).getDoubleValue());
    assertEquals(4.2, row.get(13).getDoubleValue());
    FieldValueList struct = row.get(14).getRecordValue();
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
    FieldValueList array = (FieldValueList) row.get(15).getValue();
    assertEquals(3, array.size());
    assertEquals(1, array.get(0).getLongValue());
    assertEquals(2, array.get(1).getLongValue());
    assertEquals(3, array.get(2).getLongValue());
    FieldValueList arrayOfStructs = (FieldValueList) row.get(16).getValue();
    assertEquals(1, arrayOfStructs.size());
    struct = (FieldValueList) arrayOfStructs.get(0).getValue();
    assertEquals(1L, struct.get(0).getLongValue());
    // Mixed struct
    struct = row.get(17).getRecordValue();
    assertEquals(
        4.199999809265137,
        struct.get("float_field").getDoubleValue()); // TODO: Address discrepancy here
    assertEquals("2019-03-18T01:23:45.678901", struct.get("ts_field").getStringValue());
    // Check the Map type
    FieldValueList map = (FieldValueList) row.get(18).getRepeatedValue();
    assertEquals(1, map.size());
    FieldValueList entry = map.get(0).getRecordValue();
    assertEquals("mykey", entry.get(0).getStringValue());
    assertEquals(1, entry.get(1).getRepeatedValue().size());
    FieldValueList subEntry = entry.get(1).getRepeatedValue().get(0).getRecordValue();
    assertEquals("subkey", subEntry.get(0).getStringValue());
    assertEquals(999, subEntry.get(1).getLongValue());
  }

  // ---------------------------------------------------------------------------------------------------

  /** Check that we can write NULL values in all types of fields. */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_WRITE_METHOD)
  public void testWriteAllTypesNull(String engine, String writeMethod) {
    hive.setHiveConfValue(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    initHive(engine, HiveBigQueryConfig.AVRO);
    // Create the BQ table
    createExternalTable(
        ALL_TYPES_TABLE_NAME, HIVE_ALL_TYPES_TABLE_DDL, BIGQUERY_ALL_TYPES_TABLE_DDL);
    // Insert data into the BQ table using Hive
    // TODO: Figure out why Hive won't let us insert NULL values for some fields below
    runHiveQuery(
        String.join(
            "\n",
            "INSERT INTO " + ALL_TYPES_TABLE_NAME + " VALUES(",
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
            "CAST ('' AS TIMESTAMPLOCALTZ),",
            "NULL,",
            "NULL,",
            "NULL,",
            "NAMED_STRUCT(",
            "  'min', CAST(0.0 AS" + " DECIMAL(38,9)),",
            "  'max', CAST(0.0 AS" + " DECIMAL(38,9)),",
            "  'pi', CAST(0.0 AS DECIMAL(38,9)),",
            "  'big_pi', CAST(0.0 AS" + " DECIMAL(38,9))",
            "),",
            "ARRAY(CAST (1 AS BIGINT)),",
            "ARRAY(NAMED_STRUCT('i', CAST (1 AS BIGINT))),",
            "NAMED_STRUCT('float_field', CAST(0.0 AS FLOAT), 'ts_field', CAST ('' AS TIMESTAMP)),",
            "MAP('mykey', MAP('subkey', 0))",
            ")"));
    // Read the data using the BQ SDK
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `${dataset}.%s`", ALL_TYPES_TABLE_NAME));
    // Verify we get the expected values
    assertEquals(1, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    FieldValueList row = rows.get(0);
    assertEquals(19, row.size()); // Number of columns
    // TODO: Verify the returned values
  }

  // ---------------------------------------------------------------------------------------------------

  /** Test a write operation and multiple read operations in the same query. */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_READ_FORMAT_WRITE_METHOD)
  public void testMultiReadWrite(String engine, String readDataFormat, String writeMethod) {
    hive.setHiveConfValue(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
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
  }
}
