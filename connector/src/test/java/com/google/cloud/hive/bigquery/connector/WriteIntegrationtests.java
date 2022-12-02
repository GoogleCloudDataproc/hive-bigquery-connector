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
import com.google.cloud.hive.bigquery.connector.input.BigQueryInputFormat;
import com.google.cloud.storage.Blob;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.hive.ql.io.NullRowsInputFormat;
import org.apache.hadoop.hive.ql.io.OneNullRowInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.optimizer.physical.Vectorizer;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import repackaged.by.hivebqconnector.com.google.common.collect.Streams;

public class WriteIntegrationtests extends IntegrationTestsBase {

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
                HiveBigQueryConfig.WRITE_METHOD_DIRECT,
                HiveBigQueryConfig.WRITE_METHOD_INDIRECT
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

  /** Check that we can write all types of data to BigQuery. */
  @CartesianTest
  public void testWriteAllTypes(
      @CartesianTest.Values(strings = {"mr", "tez"}) String engine,
      @CartesianTest.Values(
              strings = {
                HiveBigQueryConfig.WRITE_METHOD_DIRECT,
                HiveBigQueryConfig.WRITE_METHOD_INDIRECT
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
                "11,",
                "22,",
                "33,",
                "44,",
                "true,",
                "\"fixed char\",",
                "\"var char\",",
                "\"string\",",
                "CAST(\"2019-03-18\" AS DATE),",
                "CAST(\"2019-03-18T01:23:45.678901\" AS TIMESTAMP),",
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
                "MAP('mykey', MAP('subkey', 999))",
                "FROM (select '1') t")
            .collect(Collectors.joining("\n")));
    // Read the data using the BQ SDK
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `${dataset}.%s`", ALL_TYPES_TABLE_NAME));
    // Verify we get the expected values
    assertEquals(1, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    FieldValueList row = rows.get(0);
    assertEquals(17, row.size()); // Number of columns
    assertEquals(11L, row.get(0).getLongValue());
    assertEquals(22L, row.get(1).getLongValue());
    assertEquals(33L, row.get(2).getLongValue());
    assertEquals(44L, row.get(3).getLongValue());
    assertTrue(row.get(4).getBooleanValue());
    assertEquals("fixed char", row.get(5).getStringValue());
    assertEquals("var char", row.get(6).getStringValue());
    assertEquals("string", row.get(7).getStringValue());
    assertEquals("2019-03-18", row.get(8).getStringValue());
    if (Objects.equals(writeMethod, HiveBigQueryConfig.WRITE_METHOD_DIRECT)) {
      assertEquals(1552872225678901L, row.get(9).getTimestampValue());
    } else {
      // As we rely on the AvroSerde to generate the Avro schema for the
      // indirect write method, we lose the micro-second precision due
      // to the fact that the AvroSerde is currently limited to
      // 'timestamp-mills' precision.
      // See: https://issues.apache.org/jira/browse/HIVE-20889
      // TODO: Write our own avro schema generation tool to get
      //  around this limitation.
      assertEquals(1552872225000000L, row.get(9).getTimestampValue());
    }
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
    // Check the Map type
    FieldValueList map = (FieldValueList) row.get(16).getRepeatedValue();
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
  @CartesianTest
  public void testMultiReadWrite(
      @CartesianTest.Values(strings = {"mr", "tez"}) String engine,
      @CartesianTest.Values(strings = {HiveBigQueryConfig.ARROW, HiveBigQueryConfig.AVRO})
          String readDataFormat,
      @CartesianTest.Values(
              strings = {
                HiveBigQueryConfig.WRITE_METHOD_DIRECT,
                HiveBigQueryConfig.WRITE_METHOD_INDIRECT
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

  @Test
  public void testMerge() throws NoSuchFieldException, IllegalAccessException {
    // Client config properties:
    hive.setHiveConfValue("hive.support.concurrency", "true");
    hive.setHiveConfValue("hive.enforce.bucketing", "true");
    hive.setHiveConfValue("hive.exec.dynamic.partition.mode", "nonstrict");
    hive.setHiveConfValue("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
    // Server config properties:
    hive.setHiveConfValue("hive.compactor.initiator.on", "true");
    hive.setHiveConfValue("hive.compactor.worker.threads", "1");
    initHive();
    // Create the table
    runHiveScript(HIVE_TRANSACTIONAL_TABLE_CREATE_QUERY);

    // Hack to get around the fact that Hive hardcodes the list of supported ACID InputFormat classes
    Set<String> supportedAcidInputFormats = new TreeSet<>();
    supportedAcidInputFormats.add(OrcInputFormat.class.getName());
    supportedAcidInputFormats.add(NullRowsInputFormat.class.getName());
    supportedAcidInputFormats.add(OneNullRowInputFormat.class.getName());
    supportedAcidInputFormats.add(BigQueryInputFormat.class.getName());
    Field field = Vectorizer.class.getDeclaredField("supportedAcidInputFormats");
    field.setAccessible(true);
    Field modifiers = Field.class.getDeclaredField("modifiers");
    modifiers.setAccessible(true);
    modifiers.setInt(field, field.getModifiers() & ~Modifier.FINAL);
    field.set(null, supportedAcidInputFormats);

    runHiveScript("MERGE INTO " + TRANSACTIONAL_TABLE_NAME + " x USING (SELECT CAST (222 AS BIGINT) number, \"some text\" text) y ON x.number=y.number WHEN NOT MATCHED THEN INSERT VALUES (999, \"hello\")");
  }
}
