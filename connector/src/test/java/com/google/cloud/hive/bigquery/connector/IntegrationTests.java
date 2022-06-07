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

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.storage.*;
import com.klarna.hiverunner.*;
import com.klarna.hiverunner.builder.HiveShellBuilder;
import com.klarna.hiverunner.config.HiveRunnerConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.junit.jupiter.api.Test;
import repackaged.by.hivebqconnector.com.google.common.collect.ImmutableList;
import repackaged.by.hivebqconnector.com.google.common.collect.Streams;

// TODO: When running the tests, some noisy exceptions are displayed in the output:
//  "javax.jdo.JDOFatalUserException: Persistence Manager has been closed".
//  Those exceptions don't impact the execution of the tests, although they perhaps
//  make them run a bit slower overall. This seems related to:
//  https://issues.apache.org/jira/browse/HIVE-25261, which was fixed in Hive 4.0.0,
//  so we might have to find a workaround to make those go away with Hive 3.X.X.

public class IntegrationTests {

  private Path tmpDir;
  private HiveServerContainer hiveServerContainer; // Hive server
  private HiveShell hive; // Hive client

  public void setUp() {
    // Set up the Hive server and client
    try {
      tmpDir = Files.createTempDirectory("hiverunner_test");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    HiveServerContext context = new StandaloneHiveServerContext(tmpDir, new HiveRunnerConfig());
    hiveServerContainer = new HiveServerContainer(context);
    HiveShellBuilder hiveBuilder = new HiveShellBuilder();
    hiveBuilder.setHiveServerContainer(hiveServerContainer);
    hive = hiveBuilder.buildShell();
    // Create the test dataset and table in BigQuery
    try {
      createDataset(DATASET);
    } catch (BigQueryException e) {
      if (e.getMessage().contains("Already Exists")) {
        deleteDatasetAndTables(DATASET);
        createDataset(DATASET);
      }
    }
    // Create the bucket for 'indirect' jobs.
    try {
      createBucket(TEMP_BUCKET_NAME);
    } catch (StorageException e) {
      if (e.getCode() == 409) { // Bucket already exists
        deleteBucket(TEMP_BUCKET_NAME);
        createBucket(TEMP_BUCKET_NAME);
      }
    }
  }

  public void tearDown() {
    // Cleanup the test BQ dataset and GCS bucket
    deleteDatasetAndTables(DATASET);
    deleteBucket(TEMP_BUCKET_NAME);
    // Tear down the Hive server
    hiveServerContainer.tearDown();
    try {
      FileUtils.deleteDirectory(tmpDir.toFile());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void initHive() {
    initHive("mr", HiveBigQueryConfig.AVRO);
  }

  public void initHive(String engine, String readDataFormat) {
    initHive(engine, readDataFormat, TEMP_GCS_PATH);
  }

  public void initHive(String engine, String readDataFormat, String tempGcsPath) {
    hive.setHiveConfValue(ConfVars.HIVE_EXECUTION_ENGINE.varname, engine);
    hive.setHiveConfValue(HiveBigQueryConfig.READ_DATA_FORMAT_KEY, readDataFormat);
    hive.setHiveConfValue(HiveBigQueryConfig.TEMP_GCS_PATH_KEY, tempGcsPath);
    hive.setHiveConfValue(
        "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"); // GCS Connector
    hive.start();
    hive.execute("CREATE DATABASE source_db");
  }

  // ---------------------------------------------------------------------------------------------------

  /**
   * Check that the user didn't forget to provide all the required properties when creating a Hive
   * table.
   */
  @Test
  public void testMissingTableProperties() {
    setUp();
    initHive();
    Throwable exception =
        assertThrows(
            RuntimeException.class,
            () ->
                hive.execute(
                    String.join(
                        "\n",
                        "CREATE TABLE some_table (number BIGINT, text" + " STRING)",
                        "STORED BY"
                            + " 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler';")));
    assertTrue(
        exception
            .getMessage()
            .contains(
                "The following table property(ies) must be provided: bq.project,"
                    + " bq.dataset, bq.table"));
    tearDown();
  }

  // ---------------------------------------------------------------------------------------------------

  /** Check that we tell the user when they use unsupported Hive types. */
  @Test
  public void testUnsupportedTypes() {
    setUp();
    initHive();
    for (String type : ImmutableList.of("float", "int", "smallint", "tinyint")) {
      Throwable exception =
          assertThrows(
              RuntimeException.class,
              () ->
                  hive.execute(
                      String.join(
                          "\n",
                          "CREATE TABLE " + TEST_TABLE_NAME + " (number " + type + ")",
                          "STORED BY"
                              + " 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'")));
      assertTrue(exception.getMessage().contains("Unsupported Hive type: " + type));
    }
    tearDown();
  }

  // ---------------------------------------------------------------------------------------------------

  /** Check that reading an empty BQ table actually returns 0 results. */
  public void readEmptyTable(String engine, String readDataFormat) {
    runBqQuery(BIGQUERY_TEST_TABLE_CREATE_QUERY);
    initHive(engine, readDataFormat);
    hive.execute(HIVE_TEST_TABLE_CREATE_QUERY);
    List<Object[]> rows = hive.executeStatement(String.format("SELECT * FROM %s", TEST_TABLE_NAME));
    assertEquals(0, rows.size());
  }

  @Test
  public void testReadEmptyTable() {
    for (String engine : new String[] {"mr", "tez"}) {
      for (String readDataFormat :
          new String[] {HiveBigQueryConfig.ARROW, HiveBigQueryConfig.AVRO}) {
        setUp();
        readEmptyTable(engine, readDataFormat);
        tearDown();
      }
    }
  }

  // ---------------------------------------------------------------------------------------------------

  /** Test the WHERE clause */
  public void whereClause(String engine, String readDataFormat) {
    runBqQuery(BIGQUERY_TEST_TABLE_CREATE_QUERY);
    initHive(engine, readDataFormat);
    hive.execute(HIVE_TEST_TABLE_CREATE_QUERY);
    // Insert data into BQ using the BQ SDK
    runBqQuery(
        String.format(
            "INSERT `%s.%s` VALUES (123, 'hello'), (999, 'abcd')", DATASET, TEST_TABLE_NAME));
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `%s.%s`", DATASET, TEST_TABLE_NAME));
    // Make sure the initial data is there
    assertEquals(2, result.getTotalRows());
    // Read filtered data using Hive
    List<Object[]> rows =
        hive.executeStatement(
            String.format("SELECT * FROM %s WHERE number = 999", TEST_TABLE_NAME));
    // Verify we get the expected rows
    assertArrayEquals(
        new Object[] {
          new Object[] {999L, "abcd"},
        },
        rows.toArray());
    // TODO: Confirm that the predicate was in fact pushed down to BigQuery
  }

  @Test
  public void testWhereClause() {
    for (String engine : new String[] {"mr", "tez"}) {
      for (String readDataFormat :
          new String[] {HiveBigQueryConfig.ARROW, HiveBigQueryConfig.AVRO}) {
        setUp();
        whereClause(engine, readDataFormat);
        tearDown();
      }
    }
  }

  // ---------------------------------------------------------------------------------------------------

  /** Test the `SELECT` statement with explicit columns (i.e. not `SELECT *`) */
  public void selectExplicitColumns(String engine, String readDataFormat) {
    runBqQuery(BIGQUERY_TEST_TABLE_CREATE_QUERY);
    initHive(engine, readDataFormat);
    hive.execute(HIVE_TEST_TABLE_CREATE_QUERY);
    // Insert data into BQ using the BQ SDK
    runBqQuery(
        String.format(
            "INSERT `%s.%s` VALUES (123, 'hello'), (999, 'abcd')", DATASET, TEST_TABLE_NAME));
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `%s.%s`", DATASET, TEST_TABLE_NAME));
    // Make sure the initial data is there
    assertEquals(2, result.getTotalRows());
    // Read filtered data using Hive
    // Try with both columns in order
    List<Object[]> rows =
        hive.executeStatement(
            String.format("SELECT number, text FROM %s ORDER BY number", TEST_TABLE_NAME));
    assertArrayEquals(
        new Object[] {
          new Object[] {123L, "hello"},
          new Object[] {999L, "abcd"}
        },
        rows.toArray());
    // Try in different order
    rows =
        hive.executeStatement(
            String.format("SELECT text, number FROM %s ORDER BY number", TEST_TABLE_NAME));
    assertArrayEquals(
        new Object[] {
          new Object[] {"hello", 123L},
          new Object[] {"abcd", 999L}
        },
        rows.toArray());
    // Try a single column
    rows =
        hive.executeStatement(
            String.format("SELECT number FROM %s ORDER BY number", TEST_TABLE_NAME));
    assertArrayEquals(new Object[] {new Object[] {123L}, new Object[] {999L}}, rows.toArray());
    // Try another single column
    rows =
        hive.executeStatement(String.format("SELECT text FROM %s ORDER BY text", TEST_TABLE_NAME));
    assertArrayEquals(new Object[] {new Object[] {"abcd"}, new Object[] {"hello"}}, rows.toArray());
  }

  @Test
  public void testSelectExplicitColumns() {
    for (String engine : new String[] {"mr", "tez"}) {
      for (String readDataFormat :
          new String[] {HiveBigQueryConfig.ARROW, HiveBigQueryConfig.AVRO}) {
        setUp();
        selectExplicitColumns(engine, readDataFormat);
        tearDown();
      }
    }
  }

  // ---------------------------------------------------------------------------------------------------

  /** Smoke test to make sure BigQuery accepts all different types of pushed predicates */
  @Test
  public void testWhereClauseAllTypes() {
    setUp();
    runBqQuery(BIGQUERY_ALL_TYPES_TABLE_CREATE_QUERY);
    initHive();
    hive.execute(HIVE_ALL_TYPES_TABLE_CREATE_QUERY);
    hive.executeStatement(
        Stream.of(
                "SELECT * FROM " + ALL_TYPES_TABLE_NAME + " WHERE",
                "((int_val > 10 AND bl = TRUE)",
                "OR (str = 'hello' OR day >= to_date('2000-01-01')))",
                "AND (ts BETWEEN TIMESTAMP'2018-09-05 00:10:04.19' AND"
                    + " TIMESTAMP'2019-06-11 03:55:10.00')",
                "AND (fl <= 4.2)")
            .collect(Collectors.joining("\n")));
    tearDown();
    // TODO: Confirm that the predicates were in fact pushed down to BigQuery
  }

  // ---------------------------------------------------------------------------------------------------

  /** Insert data into a simple table. */
  public void insert(String engine, String writeMethod) {
    runBqQuery(BIGQUERY_TEST_TABLE_CREATE_QUERY);
    hive.setHiveConfValue(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    initHive(engine, HiveBigQueryConfig.AVRO);
    hive.execute(HIVE_TEST_TABLE_CREATE_QUERY);
    // Insert data using Hive
    hive.execute("INSERT INTO " + TEST_TABLE_NAME + " VALUES (123, 'hello')");
    // Read the data using the BQ SDK
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `%s.%s`", DATASET, TEST_TABLE_NAME));
    // Verify we get the expected values
    assertEquals(1, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    assertEquals(123L, rows.get(0).get(0).getLongValue());
    assertEquals("hello", rows.get(0).get(1).getStringValue());
  }

  /** Insert data using the "direct" write method. */
  @Test
  public void testInsertDirect() {
    for (String engine : new String[] {"mr", "tez"}) {
      setUp();
      insert(engine, HiveBigQueryConfig.WRITE_METHOD_DIRECT);
      tearDown();
    }
  }

  // ---------------------------------------------------------------------------------------------------

  /** Test the "INSERT OVERWRITE" statement, which clears the table before writing the new data. */
  public void insertOverwrite(String engine, String writeMethod) {
    // Create some initial data in BQ
    runBqQuery(BIGQUERY_TEST_TABLE_CREATE_QUERY);
    runBqQuery(
        String.format(
            "INSERT `%s.%s` VALUES (123, 'hello'), (999, 'abcd')", DATASET, TEST_TABLE_NAME));
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `%s.%s`", DATASET, TEST_TABLE_NAME));
    // Make sure the initial data is there
    assertEquals(2, result.getTotalRows());
    // Run INSERT OVERWRITE in Hive
    hive.setHiveConfValue(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    initHive(engine, HiveBigQueryConfig.AVRO);
    hive.execute(HIVE_TEST_TABLE_CREATE_QUERY);
    hive.execute("INSERT OVERWRITE TABLE " + TEST_TABLE_NAME + " VALUES (888, 'xyz')");
    // Make sure the new data erased the old one
    result = runBqQuery(String.format("SELECT * FROM `%s.%s`", DATASET, TEST_TABLE_NAME));
    assertEquals(1, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    assertEquals(888L, rows.get(0).get(0).getLongValue());
    assertEquals("xyz", rows.get(0).get(1).getStringValue());
  }

  @Test
  public void testInsertOverwrite() {
    for (String engine : new String[] {"mr", "tez"}) {
      for (String writeMethod :
          new String[] {
            HiveBigQueryConfig.WRITE_METHOD_DIRECT /*, HiveBigQueryConfig.WRITE_METHOD_INDIRECT*/
          }) {
        setUp();
        insertOverwrite(engine, writeMethod);
        tearDown();
      }
    }
  }

  // ---------------------------------------------------------------------------------------------------

  /** Test the "SELECT COUNT(*)" statement. */
  public void count(String engine, String readDataFormat) {
    // Create some initial data in BQ
    runBqQuery(BIGQUERY_TEST_TABLE_CREATE_QUERY);
    runBqQuery(
        String.format(
            "INSERT `%s.%s` VALUES (123, 'hello'), (999, 'abcd')", DATASET, TEST_TABLE_NAME));
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `%s.%s`", DATASET, TEST_TABLE_NAME));
    // Make sure the initial data is there
    assertEquals(2, result.getTotalRows());
    // Run COUNT query in Hive
    initHive(engine, readDataFormat);
    hive.execute(HIVE_TEST_TABLE_CREATE_QUERY);
    List<Object[]> rows = hive.executeStatement("SELECT COUNT(*) FROM " + TEST_TABLE_NAME);
    assertEquals(1, rows.size());
    assertEquals(2L, rows.get(0)[0]);
  }

  @Test
  public void testCount() {
    for (String engine : new String[] {"mr", "tez"}) {
      for (String readDataFormat :
          new String[] {HiveBigQueryConfig.ARROW, HiveBigQueryConfig.AVRO}) {
        setUp();
        count(engine, readDataFormat);
        tearDown();
      }
    }
  }

  // ---------------------------------------------------------------------------------------------------

  /** Check that we can read all types of data from BigQuery. */
  public void readAllTypes(String readDataFormat) {
    // Create the BQ table
    runBqQuery(BIGQUERY_ALL_TYPES_TABLE_CREATE_QUERY);
    // Insert data into the BQ table using the BQ SDK
    runBqQuery(
        Stream.of(
                String.format("INSERT `%s.%s` VALUES (", DATASET, ALL_TYPES_TABLE_NAME),
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
    hive.execute(HIVE_ALL_TYPES_TABLE_CREATE_QUERY);
    List<Object[]> rows = hive.executeStatement("SELECT * FROM " + ALL_TYPES_TABLE_NAME);
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

  @Test
  public void testReadAllTypes() {
    for (String readDataFormat : new String[] {HiveBigQueryConfig.ARROW, HiveBigQueryConfig.AVRO}) {
      setUp();
      readAllTypes(readDataFormat);
      tearDown();
    }
  }

  // ---------------------------------------------------------------------------------------------------

  /** Check that we can write all types of data to BigQuery. */
  public void writeAllTypes(String engine, String writeMethod) {
    // Create the BQ table
    runBqQuery(BIGQUERY_ALL_TYPES_TABLE_CREATE_QUERY);
    // Insert data into the BQ table using Hive
    hive.setHiveConfValue(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    initHive(engine, HiveBigQueryConfig.AVRO);
    hive.execute(HIVE_ALL_TYPES_TABLE_CREATE_QUERY);
    hive.execute(
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
        runBqQuery(String.format("SELECT * FROM `%s.%s`", DATASET, ALL_TYPES_TABLE_NAME));
    // Verify we get the expected values
    assertEquals(1, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    FieldValueList row = rows.get(0);
    assertEquals(10, row.size()); // Number of columns
    assertEquals(42L, row.get(0).getLongValue());
    assertTrue(row.get(1).getBooleanValue());
    assertEquals("string", row.get(2).getStringValue());
    assertEquals("2019-03-18", row.get(3).getStringValue());
    if (writeMethod == HiveBigQueryConfig.WRITE_METHOD_DIRECT) {
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

  @Test
  public void testWriteAllTypes() {
    for (String engine : new String[] {"mr", "tez"}) {
      for (String writeMethod :
          new String[] {
            HiveBigQueryConfig.WRITE_METHOD_DIRECT /*, HiveBigQueryConfig.WRITE_METHOD_INDIRECT */
          }) {
        setUp();
        writeAllTypes(engine, writeMethod);
        tearDown();
      }
    }
  }

  // ---------------------------------------------------------------------------------------------------

  /** Read from multiple tables in the same query. */
  public void multiRead(String engine, String readDataFormat) {
    // Create the BQ tables
    runBqQuery(BIGQUERY_TEST_TABLE_CREATE_QUERY);
    runBqQuery(BIGQUERY_ANOTHER_TEST_TABLE_CREATE_QUERY);
    // Insert data into the BQ tables using the BQ SDK
    runBqQuery(
        Stream.of(
                String.format("INSERT `%s.%s` VALUES", DATASET, TEST_TABLE_NAME),
                "(1, 'hello1'), (2, 'hello2'), (3, 'hello3')")
            .collect(Collectors.joining("\n")));
    runBqQuery(
        Stream.of(
                String.format("INSERT `%s.%s` VALUES", DATASET, ANOTHER_TEST_TABLE_NAME),
                "(123, 'hi123'), (42, 'hi42'), (999, 'hi999')")
            .collect(Collectors.joining("\n")));
    // Create the Hive tables
    initHive(engine, readDataFormat);
    hive.execute(HIVE_TEST_TABLE_CREATE_QUERY);
    hive.execute(HIVE_ANOTHER_TEST_TABLE_CREATE_QUERY);
    // Read from multiple table in same Hive query
    List<Object[]> rows =
        hive.executeStatement(
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

  @Test
  public void testMultiRead() {
    for (String engine : new String[] {"mr", "tez"}) {
      for (String readDataFormat :
          new String[] {HiveBigQueryConfig.ARROW, HiveBigQueryConfig.AVRO}) {
        setUp();
        multiRead(engine, readDataFormat);
        tearDown();
      }
    }
  }

  // ---------------------------------------------------------------------------------------------------

  /** Test a write operation and multiple read operations in the same query. */
  public void multiReadWrite(String engine, String readDataFormat, String writeMethod) {
    // Create the BQ tables
    runBqQuery(BIGQUERY_TEST_TABLE_CREATE_QUERY);
    runBqQuery(BIGQUERY_ANOTHER_TEST_TABLE_CREATE_QUERY);
    runBqQuery(BIGQUERY_ALL_TYPES_TABLE_CREATE_QUERY);
    // Insert data into the BQ tables using the BQ SDK
    runBqQuery(
        Stream.of(
                String.format("INSERT `%s.%s` (int_val) VALUES", DATASET, ALL_TYPES_TABLE_NAME),
                "(123), (42), (999)")
            .collect(Collectors.joining("\n")));
    runBqQuery(
        Stream.of(
                String.format("INSERT `%s.%s` (str_val) VALUES", DATASET, ANOTHER_TEST_TABLE_NAME),
                "(\"hello1\"), (\"hello2\"), (\"hello3\")")
            .collect(Collectors.joining("\n")));
    // Create the Hive tables
    hive.setHiveConfValue(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    initHive(engine, readDataFormat);
    hive.execute(HIVE_ALL_TYPES_TABLE_CREATE_QUERY);
    hive.execute(HIVE_TEST_TABLE_CREATE_QUERY);
    hive.execute(HIVE_ANOTHER_TEST_TABLE_CREATE_QUERY);
    // Read and write in the same query using Hive
    hive.execute(
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
        runBqQuery(String.format("SELECT * FROM `%s.%s`", DATASET, TEST_TABLE_NAME));
    // Verify we get the expected values
    assertEquals(1, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    assertEquals(999, rows.get(0).get(0).getLongValue());
    assertEquals("hello3", rows.get(0).get(1).getStringValue());
  }

  @Test
  public void testMultiReadWrite() {
    for (String engine : new String[] {"mr", "tez"}) {
      for (String readDataFormat :
          new String[] {HiveBigQueryConfig.ARROW, HiveBigQueryConfig.AVRO}) {
        for (String writeMethod :
            new String[] {
              HiveBigQueryConfig.WRITE_METHOD_DIRECT /*, HiveBigQueryConfig.WRITE_METHOD_INDIRECT*/
            }) {
          setUp();
          multiReadWrite(engine, readDataFormat, writeMethod);
          tearDown();
        }
      }
    }
  }

  // ---------------------------------------------------------------------------------------------------

  /** Join two tables */
  public void innerJoin(String engine, String readDataFormat) {
    // Create the BQ tables
    runBqQuery(BIGQUERY_TEST_TABLE_CREATE_QUERY);
    runBqQuery(BIGQUERY_ANOTHER_TEST_TABLE_CREATE_QUERY);
    // Insert data into the BQ tables using the BQ SDK
    runBqQuery(
        Stream.of(
                String.format("INSERT `%s.%s` VALUES", DATASET, TEST_TABLE_NAME),
                "(1, 'hello'), (2, 'bonjour'), (1, 'hola')")
            .collect(Collectors.joining("\n")));
    runBqQuery(
        Stream.of(
                String.format("INSERT `%s.%s` VALUES", DATASET, ANOTHER_TEST_TABLE_NAME),
                "(1, 'red'), (2, 'blue'), (3, 'green')")
            .collect(Collectors.joining("\n")));
    // Create the Hive tables
    initHive(engine, readDataFormat);
    hive.execute(HIVE_TEST_TABLE_CREATE_QUERY);
    hive.execute(HIVE_ANOTHER_TEST_TABLE_CREATE_QUERY);
    // Do an inner join of the two tables using Hive
    List<Object[]> rows =
        hive.executeStatement(
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

  @Test
  public void testInnerJoin() {
    for (String engine : new String[] {"mr", "tez"}) {
      for (String readDataFormat :
          new String[] {HiveBigQueryConfig.ARROW, HiveBigQueryConfig.AVRO}) {
        setUp();
        innerJoin(engine, readDataFormat);
        tearDown();
      }
    }
  }
}
