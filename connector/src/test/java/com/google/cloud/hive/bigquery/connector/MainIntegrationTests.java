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
import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.storage.*;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import repackaged.by.hivebqconnector.com.google.common.collect.ImmutableList;
import repackaged.by.hivebqconnector.com.google.common.collect.Streams;

public class MainIntegrationTests extends IntegrationTestsBase {

  /**
   * Check that the user didn't forget to provide all the required properties when creating a Hive
   * table.
   */
  @Test
  public void testMissingTableProperties() {
    initHive();
    Throwable exception =
        assertThrows(
            RuntimeException.class,
            () ->
                runHiveScript(
                    String.join(
                        "\n",
                        "CREATE TABLE some_table (number BIGINT, text" + " STRING)",
                        "STORED BY"
                            + " 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler';")));
    assertTrue(
        exception
            .getMessage()
            .contains("The following table property(ies) must be provided: bq.dataset, bq.table"));
  }

  // ---------------------------------------------------------------------------------------------------

  /** Check that the user provides a GCS temporary path when using the "indirect" write method. */
  @Test
  public void testMissingGcsTempPath() {
    hive.setHiveConfValue(
        HiveBigQueryConfig.WRITE_METHOD_KEY, HiveBigQueryConfig.WRITE_METHOD_INDIRECT);
    initHive("mr", HiveBigQueryConfig.AVRO, "");
    runHiveScript(HIVE_TEST_TABLE_CREATE_QUERY);
    Throwable exception =
        assertThrows(
            RuntimeException.class,
            () -> runHiveScript("INSERT INTO " + TEST_TABLE_NAME + " VALUES (123, 'hello')"));
    assertTrue(
        exception
            .getMessage()
            .contains(
                "The 'bq.temp.gcs.path' property must be set when using the"
                    + " 'indirect' write method."));
  }

  /**
   * Check that the user has proper write permissions to the provided GCS temporary path when using
   * the "indirect" write method.
   */
  @Test
  public void testMissingBucketPermissions() {
    hive.setHiveConfValue(
        HiveBigQueryConfig.WRITE_METHOD_KEY, HiveBigQueryConfig.WRITE_METHOD_INDIRECT);
    initHive("mr", HiveBigQueryConfig.AVRO, "gs://random-bucket-abcdef-12345");
    runHiveScript(HIVE_TEST_TABLE_CREATE_QUERY);
    Throwable exception =
        assertThrows(
            RuntimeException.class,
            () -> runHiveScript("INSERT INTO " + TEST_TABLE_NAME + " VALUES (123, 'hello')"));
    // TODO: Look into why we don't always get the same message back
    String message1 = "Cannot write to table 'test'. Does not have write access to the following GCS path, or bucket does not exist: gs://random-bucket-abcdef-12345)";
    String message2 = "Cannot write to table 'test'. The service account does not have IAM permissions to write to the following GCS path, or bucket does not exist: gs://random-bucket-abcdef-12345";
    assertTrue(
        exception
            .getMessage()
            .contains(message1) ||
          exception
              .getMessage()
              .contains(message2)
    );
  }

  // ---------------------------------------------------------------------------------------------------

  /** Check that we tell the user when they use unsupported Hive types. */
  @Test
  public void testUnsupportedTypes() {
    initHive();
    for (String type : ImmutableList.of("float", "int", "smallint", "tinyint")) {
      Throwable exception =
          assertThrows(
              RuntimeException.class,
              () ->
                  runHiveScript(
                      String.join(
                          "\n",
                          "CREATE TABLE " + TEST_TABLE_NAME + " (number " + type + ")",
                          "STORED BY"
                              + " 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'")));
      assertTrue(exception.getMessage().contains("Unsupported Hive type: " + type));
    }
  }

  // -----------------------------------------------------------------------------------------------

  /** Check that attempting to read a table that doesn't exist fails gracefully with a useful error message */
  @CartesianTest
  public void testReadNonExistingTable(
      @CartesianTest.Values(strings = {"mr", "tez"}) String engine,
      @CartesianTest.Values(strings = {HiveBigQueryConfig.ARROW, HiveBigQueryConfig.AVRO})
          String readDataFormat) {
    // Make sure the table doesn't exist in BigQuery
    dropBqTableIfExists(dataset, TEST_TABLE_NAME);
    assertFalse(bQTableExists(dataset, TEST_TABLE_NAME));
    // Create a Hive table without creating its corresponding table in BigQuery
    initHive(engine, readDataFormat);
    runHiveScript(HIVE_TEST_TABLE_CREATE_QUERY);
    // Attempt to read the table
    Throwable exception =
        assertThrows(
            RuntimeException.class,
            () -> runHiveStatement(String.format("SELECT * FROM %s", TEST_TABLE_NAME)));
    assertTrue(exception.getMessage().contains(
        String.format("Table '%s.%s.%s' not found", getProject(), dataset, TEST_TABLE_NAME)));
  }

  // ---------------------------------------------------------------------------------------------------

  /** Check that reading an empty BQ table actually returns 0 results. */
  @CartesianTest
  public void testReadEmptyTable(
      @CartesianTest.Values(strings = {"mr", "tez"}) String engine,
      @CartesianTest.Values(strings = {HiveBigQueryConfig.ARROW, HiveBigQueryConfig.AVRO})
          String readDataFormat) {
    runBqQuery(BIGQUERY_TEST_TABLE_CREATE_QUERY);
    initHive(engine, readDataFormat);
    runHiveScript(HIVE_TEST_TABLE_CREATE_QUERY);
    List<Object[]> rows = runHiveStatement(String.format("SELECT * FROM %s", TEST_TABLE_NAME));
    assertThat(rows).isEmpty();
  }

  // ---------------------------------------------------------------------------------------------------

  /** Test the WHERE clause */
  @CartesianTest
  public void testWhereClause(
      @CartesianTest.Values(strings = {"mr", "tez"}) String engine,
      @CartesianTest.Values(strings = {HiveBigQueryConfig.ARROW, HiveBigQueryConfig.AVRO})
          String readDataFormat) {
    runBqQuery(BIGQUERY_TEST_TABLE_CREATE_QUERY);
    initHive(engine, readDataFormat);
    runHiveScript(HIVE_TEST_TABLE_CREATE_QUERY);
    // Insert data into BQ using the BQ SDK
    runBqQuery(
        String.format(
            "INSERT `${dataset}.%s` VALUES (123, 'hello'), (999, 'abcd')", TEST_TABLE_NAME));
    // Make sure the initial data is there
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `${dataset}.%s`", TEST_TABLE_NAME));
    assertEquals(2, result.getTotalRows());
    // Read filtered data using Hive
    List<Object[]> rows =
        runHiveStatement(
            String.format("SELECT * FROM %s WHERE number = 999", TEST_TABLE_NAME));
    // Verify we get the expected rows
    assertArrayEquals(
        new Object[] {
            new Object[] {999L, "abcd"},
        },
        rows.toArray());
    // TODO: Confirm that the predicate was in fact pushed down to BigQuery
  }

  // ---------------------------------------------------------------------------------------------------

  /** Test the `SELECT` statement with explicit columns (i.e. not `SELECT *`) */
  @CartesianTest
  public void testSelectExplicitColumns(
      @CartesianTest.Values(strings = {"mr", "tez"}) String engine,
      @CartesianTest.Values(strings = {HiveBigQueryConfig.ARROW, HiveBigQueryConfig.AVRO})
          String readDataFormat) {
    runBqQuery(BIGQUERY_TEST_TABLE_CREATE_QUERY);
    initHive(engine, readDataFormat);
    runHiveScript(HIVE_TEST_TABLE_CREATE_QUERY);
    // Insert data into BQ using the BQ SDK
    runBqQuery(
        String.format(
            "INSERT `${dataset}.%s` VALUES (123, 'hello'), (999, 'abcd')", TEST_TABLE_NAME));
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `${dataset}.%s`", TEST_TABLE_NAME));
    // Make sure the initial data is there
    assertEquals(2, result.getTotalRows());
    // Read filtered data using Hive
    // Try with both columns in order
    List<Object[]> rows =
        runHiveStatement(
            String.format("SELECT number, text FROM %s ORDER BY number", TEST_TABLE_NAME));
    assertArrayEquals(
        new Object[] {
            new Object[] {123L, "hello"},
            new Object[] {999L, "abcd"}
        },
        rows.toArray());
    // Try in different order
    rows =
        runHiveStatement(
            String.format("SELECT text, number FROM %s ORDER BY number", TEST_TABLE_NAME));
    assertArrayEquals(
        new Object[] {
            new Object[] {"hello", 123L},
            new Object[] {"abcd", 999L}
        },
        rows.toArray());
    // Try a single column
    rows =
        runHiveStatement(
            String.format("SELECT number FROM %s ORDER BY number", TEST_TABLE_NAME));
    assertArrayEquals(new Object[] {new Object[] {123L}, new Object[] {999L}}, rows.toArray());
    // Try another single column
    rows =
        runHiveStatement(String.format("SELECT text FROM %s ORDER BY text", TEST_TABLE_NAME));
    assertArrayEquals(new Object[] {new Object[] {"abcd"}, new Object[] {"hello"}}, rows.toArray());
  }

  // ---------------------------------------------------------------------------------------------------

  /** Smoke test to make sure BigQuery accepts all different types of pushed predicates */
  @Test
  public void testWhereClauseAllTypes() {
    runBqQuery(BIGQUERY_ALL_TYPES_TABLE_CREATE_QUERY);
    initHive();
    runHiveScript(HIVE_ALL_TYPES_TABLE_CREATE_QUERY);
    runHiveScript(
        Stream.of(
                "SELECT * FROM " + ALL_TYPES_TABLE_NAME + " WHERE",
                "((int_val > 10 AND bl = TRUE)",
                "OR (str = 'hello' OR day >= to_date('2000-01-01')))",
                "AND (ts BETWEEN TIMESTAMP'2018-09-05 00:10:04.19' AND"
                    + " TIMESTAMP'2019-06-11 03:55:10.00')",
                "AND (fl <= 4.2)")
            .collect(Collectors.joining("\n")));
    // TODO: Confirm that the predicates were in fact pushed down to BigQuery
  }

  // ---------------------------------------------------------------------------------------------------

  /** Insert data into a simple table. */
  public void insert(String engine, String writeMethod) {
    runBqQuery(BIGQUERY_TEST_TABLE_CREATE_QUERY);
    hive.setHiveConfValue(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    initHive(engine, HiveBigQueryConfig.AVRO);
    runHiveScript(HIVE_TEST_TABLE_CREATE_QUERY);
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
  @CartesianTest
  public void testInsertDirect(@CartesianTest.Values(strings = {"mr", "tez"}) String engine) {
    insert(engine, HiveBigQueryConfig.WRITE_METHOD_DIRECT);
  }

  // ---------------------------------------------------------------------------------------------------

  /** Insert data using the "indirect" write method. */
  @CartesianTest
  public void testInsertIndirect(@CartesianTest.Values(strings = {"mr", "tez"}) String engine) {
    // Check that the bucket is empty
    List<Blob> blobs = getBlobs(getIndirectWriteBucket());
    assertEquals(0, blobs.size());

    // Insert data using Hive
    insert(engine, HiveBigQueryConfig.WRITE_METHOD_INDIRECT);

    // Check that the blob was created by the job.
    blobs = getBlobs(getIndirectWriteBucket());
    assertEquals(1, blobs.size());
    assertTrue(
        blobs.get(0).getName().startsWith("temp/bq-hive-")
            && blobs.get(0).getName().endsWith(".avro"));
  }

  // ---------------------------------------------------------------------------------------------------

  /** Test the "INSERT OVERWRITE" statement, which clears the table before writing the new data. */
  @CartesianTest
  public void testInsertOverwrite(
      @CartesianTest.Values(strings = {"mr", "tez"}) String engine,
      @CartesianTest.Values(
          strings = {
              HiveBigQueryConfig
                  .WRITE_METHOD_DIRECT, HiveBigQueryConfig.WRITE_METHOD_INDIRECT
          })
          String writeMethod) {
    // Create some initial data in BQ
    runBqQuery(BIGQUERY_TEST_TABLE_CREATE_QUERY);
    runBqQuery(
        String.format(
            "INSERT `${dataset}.%s` VALUES (123, 'hello'), (999, 'abcd')", TEST_TABLE_NAME));
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `${dataset}.%s`", TEST_TABLE_NAME));
    // Make sure the initial data is there
    assertEquals(2, result.getTotalRows());
    // Run INSERT OVERWRITE in Hive
    hive.setHiveConfValue(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    initHive(engine, HiveBigQueryConfig.AVRO);
    runHiveScript(HIVE_TEST_TABLE_CREATE_QUERY);
    runHiveScript("INSERT OVERWRITE TABLE " + TEST_TABLE_NAME + " VALUES (888, 'xyz')");
    // Make sure the new data erased the old one
    result = runBqQuery(String.format("SELECT * FROM `${dataset}.%s`", TEST_TABLE_NAME));
    assertEquals(1, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    assertEquals(888L, rows.get(0).get(0).getLongValue());
    assertEquals("xyz", rows.get(0).get(1).getStringValue());
  }

  // ---------------------------------------------------------------------------------------------------

  /** Test the "SELECT COUNT(*)" statement. */
  @CartesianTest
  public void testCount(
      @CartesianTest.Values(strings = {"mr", "tez"}) String engine,
      @CartesianTest.Values(strings = {HiveBigQueryConfig.ARROW, HiveBigQueryConfig.AVRO})
          String readDataFormat) {
    // Create some initial data in BQ
    runBqQuery(BIGQUERY_TEST_TABLE_CREATE_QUERY);
    runBqQuery(
        String.format(
            "INSERT `${dataset}.%s` VALUES (123, 'hello'), (999, 'abcd')", TEST_TABLE_NAME));
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `${dataset}.%s`", TEST_TABLE_NAME));
    // Make sure the initial data is there
    assertEquals(2, result.getTotalRows());
    // Run COUNT query in Hive
    initHive(engine, readDataFormat);
    runHiveScript(HIVE_TEST_TABLE_CREATE_QUERY);
    List<Object[]> rows = runHiveStatement("SELECT COUNT(*) FROM " + TEST_TABLE_NAME);
    assertEquals(1, rows.size());
    assertEquals(2L, rows.get(0)[0]);
  }

  // ---------------------------------------------------------------------------------------------------

  /** Check that we can read all types of data from BigQuery. */
  @CartesianTest
  public void testReadAllTypes(
      @CartesianTest.Values(strings = {HiveBigQueryConfig.ARROW, HiveBigQueryConfig.AVRO})
          String readDataFormat) {
    // Create the BQ table
    runBqQuery(BIGQUERY_ALL_TYPES_TABLE_CREATE_QUERY);
    // Insert data into the BQ table using the BQ SDK
    runBqQuery(
        Stream.of(
                String.format("INSERT `${dataset}.%s` VALUES (", ALL_TYPES_TABLE_NAME),
                "42,",
                "true,",
                "\"string\",",
                "cast(\"2019-03-18\" as date),",
                "cast(\"2019-03-18T01:23:45.678901\" as timestamp),",
                "cast(\"bytes\" as bytes),",
                "4.2,",
                "struct(",
                "  cast(\"-99999999999999999999999999999.999999999\" as numeric),",
                "  cast(\"99999999999999999999999999999.999999999\" as numeric),",
                "  cast(3.14 as numeric),",
                "  cast(\"31415926535897932384626433832.795028841\" as numeric)",
                "),",
                "[1, 2, 3],",
                "[(select as struct 1)]",
                ")")
            .collect(Collectors.joining("\n")));
    // Read the data using Hive
    initHive("mr", readDataFormat);
    runHiveScript(HIVE_ALL_TYPES_TABLE_CREATE_QUERY);
    List<Object[]> rows = runHiveStatement("SELECT * FROM " + ALL_TYPES_TABLE_NAME);
    assertEquals(1, rows.size());
    Object[] row = rows.get(0);
    assertEquals(10, row.length); // Number of columns
    assertEquals(42L, (long) row[0]);
    assertEquals(true, row[1]);
    assertEquals("string", row[2]);
    assertEquals("2019-03-18", row[3]);
    assertEquals("2019-03-18 01:23:45.678901", row[4]);
    assertArrayEquals("bytes".getBytes(), (byte[]) row[5]);
    assertEquals(4.2, row[6]);
    assertEquals(
        "{\"min\":-99999999999999999999999999999.999999999,\"max\":99999999999999999999999999999.999999999,\"pi\":3.14,\"big_pi\":31415926535897932384626433832.795028841}",
        row[7]);
    assertEquals("[1,2,3]", row[8]);
    assertEquals("[{\"i\":1}]", row[9]);
  }

  // ---------------------------------------------------------------------------------------------------

  /** Check that we can write all types of data to BigQuery. */
  @CartesianTest
  public void testWriteAllTypes(
      @CartesianTest.Values(strings = {"mr", "tez"}) String engine,
      @CartesianTest.Values(
          strings = {
              HiveBigQueryConfig
                  .WRITE_METHOD_DIRECT, HiveBigQueryConfig.WRITE_METHOD_INDIRECT
          })
          String writeMethod) {
    // Create the BQ table
    runBqQuery(BIGQUERY_ALL_TYPES_TABLE_CREATE_QUERY);
    // Insert data into the BQ table using Hive
    hive.setHiveConfValue(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    initHive(engine, HiveBigQueryConfig.AVRO);
    runHiveScript(HIVE_ALL_TYPES_TABLE_CREATE_QUERY);
    runHiveScript(
        Stream.of(
                "INSERT INTO " + ALL_TYPES_TABLE_NAME + " SELECT",
                "42,",
                "true,",
                "\"string\",",
                "CAST(\"2019-03-18\" AS DATE),",
                "CAST(\"2019-03-18T01:23:45.678901\" AS TIMESTAMP),",
                "CAST(\"bytes\" AS BINARY),",
                "4.2,",
                "NAMED_STRUCT(",
                "  'min', CAST(-99999999999999999999999999999.999999999 AS" + " DECIMAL(38,9)),",
                "  'max', CAST(99999999999999999999999999999.999999999 AS" + " DECIMAL(38,9)),",
                "  'pi', CAST(3.14 AS DECIMAL(38,9)),",
                "  'big_pi', CAST(31415926535897932384626433832.795028841 AS" + " DECIMAL(38,9))",
                "),",
                "ARRAY(CAST (1 AS BIGINT), CAST (2 AS BIGINT), CAST (3 AS" + " BIGINT)),",
                "ARRAY(NAMED_STRUCT('i', CAST (1 AS BIGINT)))",
                "FROM (select '1') t")
            .collect(Collectors.joining("\n")));
    // Read the data using the BQ SDK
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `${dataset}.%s`", ALL_TYPES_TABLE_NAME));
    // Verify we get the expected values
    assertEquals(1, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    FieldValueList row = rows.get(0);
    assertEquals(10, row.size()); // Number of columns
    assertEquals(42L, row.get(0).getLongValue());
    assertTrue(row.get(1).getBooleanValue());
    assertEquals("string", row.get(2).getStringValue());
    assertEquals("2019-03-18", row.get(3).getStringValue());
    if (Objects.equals(writeMethod, HiveBigQueryConfig.WRITE_METHOD_DIRECT)) {
      assertEquals(1552872225678901L, row.get(4).getTimestampValue());
    } else {
      // As we rely on the AvroSerde to generate the Avro schema for the
      // indirect write method, we lose the micro-second precision due
      // to the fact that the AvroSerde is currently limited to
      // 'timestamp-mills' precision.
      // See: https://issues.apache.org/jira/browse/HIVE-20889
      // TODO: Write our own avro schema generation tool to get
      //  around this limitation.
      assertEquals(1552872225000000L, row.get(4).getTimestampValue());
    }
    assertArrayEquals("bytes".getBytes(), row.get(5).getBytesValue());
    assertEquals(4.2, row.get(6).getDoubleValue());
    FieldValueList struct = row.get(7).getRecordValue();
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
    FieldValueList array = (FieldValueList) row.get(8).getValue();
    assertEquals(3, array.size());
    assertEquals(1, array.get(0).getLongValue());
    assertEquals(2, array.get(1).getLongValue());
    assertEquals(3, array.get(2).getLongValue());
    FieldValueList arrayOfStructs = (FieldValueList) row.get(9).getValue();
    assertEquals(1, arrayOfStructs.size());
    struct = (FieldValueList) arrayOfStructs.get(0).getValue();
    assertEquals(1L, struct.get(0).getLongValue());
  }

  // ---------------------------------------------------------------------------------------------------

  /** Read from multiple tables in the same query. */
  @CartesianTest
  public void testMultiRead(
      @CartesianTest.Values(strings = {"mr", "tez"}) String engine,
      @CartesianTest.Values(strings = {HiveBigQueryConfig.ARROW, HiveBigQueryConfig.AVRO})
          String readDataFormat) {
    // Create the BQ tables
    runBqQuery(BIGQUERY_TEST_TABLE_CREATE_QUERY);
    runBqQuery(BIGQUERY_ANOTHER_TEST_TABLE_CREATE_QUERY);
    // Insert data into the BQ tables using the BQ SDK
    runBqQuery(
        Stream.of(
                String.format("INSERT `${dataset}.%s` VALUES", TEST_TABLE_NAME),
                "(1, 'hello1'), (2, 'hello2'), (3, 'hello3')")
            .collect(Collectors.joining("\n")));
    runBqQuery(
        Stream.of(
                String.format("INSERT `${dataset}.%s` VALUES", ANOTHER_TEST_TABLE_NAME),
                "(123, 'hi123'), (42, 'hi42'), (999, 'hi999')")
            .collect(Collectors.joining("\n")));
    // Create the Hive tables
    initHive(engine, readDataFormat);
    runHiveScript(HIVE_TEST_TABLE_CREATE_QUERY);
    runHiveScript(HIVE_ANOTHER_TEST_TABLE_CREATE_QUERY);
    // Read from multiple table in same Hive query
    List<Object[]> rows =
        runHiveStatement(
            Stream.of(
                    "SELECT",
                    "*",
                    "FROM (",
                    "SELECT",
                    "t1.num as number,",
                    "t1.str_val as text",
                    "FROM " + ANOTHER_TEST_TABLE_NAME + " t1",
                    "UNION ALL",
                    "SELECT *",
                    "FROM " + TEST_TABLE_NAME + " t2",
                    ") unioned_table ORDER BY number")
                .collect(Collectors.joining("\n")));
    assertArrayEquals(
        new Object[] {
            new Object[] {1L, "hello1"},
            new Object[] {2L, "hello2"},
            new Object[] {3L, "hello3"},
            new Object[] {42L, "hi42"},
            new Object[] {123L, "hi123"},
            new Object[] {999L, "hi999"},
        },
        rows.toArray());
  }

  // ---------------------------------------------------------------------------------------------------

  /** Test a write operation and multiple read operations in the same query. */
  @CartesianTest
  public void testMultiReadWrite(
      @CartesianTest.Values(strings = {"mr", "tez"}) String engine,
      @CartesianTest.Values(strings = {HiveBigQueryConfig.ARROW, HiveBigQueryConfig.AVRO}) String readDataFormat,
      @CartesianTest.Values(
          strings = {
              HiveBigQueryConfig
                  .WRITE_METHOD_DIRECT, HiveBigQueryConfig.WRITE_METHOD_INDIRECT
          })
          String writeMethod) {
    // Create the BQ tables
    runBqQuery(BIGQUERY_TEST_TABLE_CREATE_QUERY);
    runBqQuery(BIGQUERY_ANOTHER_TEST_TABLE_CREATE_QUERY);
    runBqQuery(BIGQUERY_ALL_TYPES_TABLE_CREATE_QUERY);
    // Insert data into the BQ tables using the BQ SDK
    runBqQuery(
        Stream.of(
                String.format("INSERT `${dataset}.%s` (int_val) VALUES", ALL_TYPES_TABLE_NAME),
                "(123), (42), (999)")
            .collect(Collectors.joining("\n")));
    runBqQuery(
        Stream.of(
                String.format("INSERT `${dataset}.%s` (str_val) VALUES", ANOTHER_TEST_TABLE_NAME),
                "(\"hello1\"), (\"hello2\"), (\"hello3\")")
            .collect(Collectors.joining("\n")));
    // Create the Hive tables
    hive.setHiveConfValue(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    initHive(engine, readDataFormat);
    runHiveScript(HIVE_ALL_TYPES_TABLE_CREATE_QUERY);
    runHiveScript(HIVE_TEST_TABLE_CREATE_QUERY);
    runHiveScript(HIVE_ANOTHER_TEST_TABLE_CREATE_QUERY);
    // Read and write in the same query using Hive
    runHiveScript(
        Stream.of(
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
                ") unioned;")
            .collect(Collectors.joining("\n")));
    // Read the result using the BQ SDK
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `${dataset}.%s`", TEST_TABLE_NAME));
    // Verify we get the expected values
    assertEquals(1, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    assertEquals(999, rows.get(0).get(0).getLongValue());
    assertEquals("hello3", rows.get(0).get(1).getStringValue());
  }

  // ---------------------------------------------------------------------------------------------------

  /** Join two tables */
  @CartesianTest
  public void testInnerJoin(
      @CartesianTest.Values(strings = {"mr", "tez"}) String engine,
      @CartesianTest.Values(strings = {HiveBigQueryConfig.ARROW, HiveBigQueryConfig.AVRO})
          String readDataFormat) {
    // Create the BQ tables
    runBqQuery(BIGQUERY_TEST_TABLE_CREATE_QUERY);
    runBqQuery(BIGQUERY_ANOTHER_TEST_TABLE_CREATE_QUERY);
    // Insert data into the BQ tables using the BQ SDK
    runBqQuery(
        Stream.of(
                String.format("INSERT `${dataset}.%s` VALUES", TEST_TABLE_NAME),
                "(1, 'hello'), (2, 'bonjour'), (1, 'hola')")
            .collect(Collectors.joining("\n")));
    runBqQuery(
        Stream.of(
                String.format("INSERT `${dataset}.%s` VALUES", ANOTHER_TEST_TABLE_NAME),
                "(1, 'red'), (2, 'blue'), (3, 'green')")
            .collect(Collectors.joining("\n")));
    // Create the Hive tables
    initHive(engine, readDataFormat);
    runHiveScript(HIVE_TEST_TABLE_CREATE_QUERY);
    runHiveScript(HIVE_ANOTHER_TEST_TABLE_CREATE_QUERY);
    // Do an inner join of the two tables using Hive
    List<Object[]> rows =
        runHiveStatement(
            Stream.of(
                    "SELECT",
                    "t2.number,",
                    "t1.str_val,",
                    "t2.text",
                    "FROM " + ANOTHER_TEST_TABLE_NAME + " t1",
                    "JOIN " + TEST_TABLE_NAME + " t2",
                    "ON (t1.num = t2.number)",
                    "ORDER BY t2.number, t1.str_val, t2.text")
                .collect(Collectors.joining("\n")));
    assertArrayEquals(
        new Object[] {
            new Object[] {1L, "red", "hello"},
            new Object[] {1L, "red", "hola"},
            new Object[] {2L, "blue", "bonjour"},
        },
        rows.toArray());
  }
}