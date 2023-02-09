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
import static com.google.cloud.hive.bigquery.connector.TestUtils.TEST_TABLE_NAME;
import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.storage.Blob;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import repackaged.by.hivebqconnector.com.google.common.collect.Streams;

public class WriteIntegrationTests extends IntegrationTestsBase {

  // ---------------------------------------------------------------------------------------------------

  /** Insert data into a simple table. */
  public void insert(String engine, String writeMethod) {
    hive.setHiveConfValue(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    initHive(engine, HiveBigQueryConfig.AVRO);
    createExternalTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL, BIGQUERY_TEST_TABLE_DDL);
    // Insert data using Hive
    runHiveScript("INSERT INTO " + TEST_TABLE_NAME + " VALUES (123, 'hello')");
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
    insert(engine, HiveBigQueryConfig.WRITE_METHOD_DIRECT);
  }

  // ---------------------------------------------------------------------------------------------------

  /** Insert data using the "indirect" write method. */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE)
  public void testInsertIndirect(String engine) {
    // Check that the bucket is empty
    List<Blob> blobs = getBlobs(getTestBucket());
    assertEquals(0, blobs.size());

    // Insert data using Hive
    insert(engine, HiveBigQueryConfig.WRITE_METHOD_INDIRECT);

    // Check that the blob was created by the job.
    blobs = getBlobs(getTestBucket());
    assertEquals(1, blobs.size());
    assertTrue(
        blobs.get(0).getName().startsWith("temp/bq-hive-")
            && blobs.get(0).getName().endsWith(".avro"));
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
    runHiveScript("INSERT OVERWRITE TABLE " + TEST_TABLE_NAME + " VALUES (888, 'xyz')");
    // Make sure the new data erased the old one
    result = runBqQuery(String.format("SELECT * FROM `${dataset}.%s`", TEST_TABLE_NAME));
    assertEquals(1, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    assertEquals(888L, rows.get(0).get(0).getLongValue());
    assertEquals("xyz", rows.get(0).get(1).getStringValue());
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
    runHiveScript(
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
    runHiveScript(
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
            ") unioned;"));
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
