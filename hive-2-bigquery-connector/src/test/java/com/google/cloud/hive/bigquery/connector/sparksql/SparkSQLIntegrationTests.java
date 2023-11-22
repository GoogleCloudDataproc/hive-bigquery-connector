/*
 * Copyright 2023 Google Inc. All Rights Reserved.
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
package com.google.cloud.hive.bigquery.connector.sparksql;

import static com.google.cloud.hive.bigquery.connector.sparksql.SparkTestUtils.DerbyDiskDB;
import static com.google.cloud.hive.bigquery.connector.sparksql.SparkTestUtils.getSparkSession;
import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.hive.bigquery.connector.TestUtils;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.integration.IntegrationTestsBase;
import com.google.common.collect.Streams;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import scala.collection.immutable.Map;
import scala.collection.mutable.WrappedArray;

public class SparkSQLIntegrationTests extends IntegrationTestsBase {

  public Row[] runSparkSQLQuery(DerbyDiskDB derby, String queryTemplate) {
    SparkSession spark = getSparkSession(derby, hive.getHiveConf());
    Dataset<Row> ds = spark.sql(renderQueryTemplate(queryTemplate));
    Row[] rows = (Row[]) ds.collect();
    spark.stop();
    return rows;
  }

  @ParameterizedTest
  @MethodSource(IntegrationTestsBase.EXECUTION_ENGINE_READ_FORMAT)
  public void testWhereClause(String engine, String readDataFormat) {
    DerbyDiskDB derby = new DerbyDiskDB(hive);
    initHive(engine, readDataFormat);
    createExternalTable(
        TestUtils.TEST_TABLE_NAME,
        TestUtils.HIVE_TEST_TABLE_DDL,
        TestUtils.BIGQUERY_TEST_TABLE_DDL);
    // Insert data into BQ using the BQ SDK
    runBqQuery(
        String.format(
            "INSERT `${dataset}.%s` VALUES (123, 'hello'), (999, 'abcd')",
            TestUtils.TEST_TABLE_NAME));
    // Read data with Spark SQL
    Row[] rows =
        runSparkSQLQuery(
            derby,
            String.format(
                "SELECT * FROM default.%s WHERE number = 999", TestUtils.TEST_TABLE_NAME));
    assertArrayEquals(
        new Object[] {
          new Object[] {999L, "abcd"},
        },
        SparkTestUtils.simplifySparkRows(rows));
  }

  // ---------------------------------------------------------------------------------------------------

  /** Check that we can read all types of data from BigQuery. */
  @ParameterizedTest
  @MethodSource(IntegrationTestsBase.READ_FORMAT)
  public void testReadAllTypes(String readDataFormat) throws IOException {
    DerbyDiskDB derby = new DerbyDiskDB(hive);
    initHive(IntegrationTestsBase.getDefaultExecutionEngine(), readDataFormat);
    createExternalTable(
        TestUtils.ALL_TYPES_TABLE_NAME,
        TestUtils.HIVE_ALL_TYPES_TABLE_DDL,
        TestUtils.BIGQUERY_ALL_TYPES_TABLE_DDL);
    // Insert data into the BQ table using the BQ SDK
    runBqQuery(
        String.join(
            "\n",
            String.format("INSERT `${dataset}.%s` VALUES (", TestUtils.ALL_TYPES_TABLE_NAME),
            "11,",
            "22,",
            "33,",
            "44,",
            "true,",
            "\"fixed char\",",
            "\"var char\",",
            "\"string\",",
            "cast(\"2019-03-18\" as date),",
            // Wall clock (no timezone)
            "cast(\"2000-01-01T00:23:45.123456\" as datetime),",
            "cast(\"bytes\" as bytes),",
            "2.0,",
            "4.2,",
            "struct(",
            "  cast(\"-99999999999999999999999999999.999999999\" as numeric),",
            "  cast(\"99999999999999999999999999999.999999999\" as numeric),",
            "  cast(3.14 as numeric),",
            "  cast(\"31415926535897932384626433832.795028841\" as numeric)",
            "),",
            "[1, 2, 3],",
            "[(select as struct 111), (select as struct 222), (select as struct 333)],",
            "struct(4.2, cast(\"2019-03-18 11:23:45.678901\" as datetime)),",
            "[struct('a_key', [struct('a_subkey', 888)]), struct('b_key', [struct('b_subkey',"
                + " 999)])]",
            ")"));
    // Read the data using Spark SQL
    Row[] rows = runSparkSQLQuery(derby, "SELECT * FROM default." + TestUtils.ALL_TYPES_TABLE_NAME);
    assertEquals(1, rows.length);
    Row row = rows[0];
    assertEquals(18, row.size()); // Number of columns
    assertEquals((byte) 11, row.get(0));
    assertEquals((short) 22, row.get(1));
    assertEquals((int) 33, row.get(2));
    assertEquals((long) 44, row.get(3));
    assertEquals(true, row.get(4));
    assertEquals("fixed char", row.get(5));
    assertEquals("var char", row.get(6));
    assertEquals("string", row.get(7));
    assertEquals("2019-03-18", row.get(8).toString());
    assertEquals("2000-01-01 00:23:45.123456", row.get(9).toString());
    assertArrayEquals("bytes".getBytes(), (byte[]) row.get(10));
    assertEquals(2.0, row.getFloat(11));
    assertEquals(4.2, row.getDouble(12));
    assertEquals(
        "{min=-99999999999999999999999999999.999999999, max=99999999999999999999999999999.999999999, pi=3.140000000, big_pi=31415926535897932384626433832.795028841}",
        SparkTestUtils.convertSparkRowToMap((GenericRowWithSchema) row.get(13)).toString());
    assertArrayEquals(
        new Long[] {1l, 2l, 3l}, SparkTestUtils.convertSparkArray((WrappedArray) row.get(14)));
    assertEquals(
        "{i=111},{i=222},{i=333}",
        Arrays.stream(SparkTestUtils.convertSparkArray((WrappedArray) row.get(15)))
            .map(s -> s.toString())
            .collect(Collectors.joining(",")));
    assertEquals(
        "{float_field=4.2, ts_field=2019-03-18 11:23:45.678901}",
        SparkTestUtils.convertSparkRowToMap((GenericRowWithSchema) row.get(16)).toString());
    // Map type
    Map map = (Map) row.get(17);
    assertEquals(2, map.size());
    assertEquals(888, ((Map) map.get("a_key").get()).get("a_subkey").get());
    assertEquals(999, ((Map) map.get("b_key").get()).get("b_subkey").get());
  }

  // ---------------------------------------------------------------------------------------------------

  /** Check that we can write all types of data to BigQuery. */
  @ParameterizedTest
  @MethodSource(IntegrationTestsBase.EXECUTION_ENGINE_WRITE_METHOD)
  public void testWriteAllTypes(String engine, String writeMethod) {
    DerbyDiskDB derby = new DerbyDiskDB(hive);
    hive.setHiveConfValue(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    initHive(engine, HiveBigQueryConfig.AVRO);
    // Create the BQ table
    createExternalTable(
        TestUtils.ALL_TYPES_TABLE_NAME,
        TestUtils.HIVE_ALL_TYPES_TABLE_DDL,
        TestUtils.BIGQUERY_ALL_TYPES_TABLE_DDL);
    // Insert data into the BQ table using Spark SQL
    SparkSession spark = getSparkSession(derby, hive.getHiveConf());
    spark.sql(
        String.join(
            "\n",
            "INSERT INTO " + TestUtils.ALL_TYPES_TABLE_NAME + " SELECT",
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
            "  'min', CAST(-99999999999999999999999999999.999999999 AS" + " DECIMAL(38,9)),",
            "  'max', CAST(99999999999999999999999999999.999999999 AS" + " DECIMAL(38,9)),",
            "  'pi', CAST(3.14 AS DECIMAL(38,9)),",
            "  'big_pi', CAST(31415926535897932384626433832.795028841 AS" + " DECIMAL(38,9))",
            "),",
            "ARRAY(CAST (1 AS BIGINT), CAST (2 AS BIGINT), CAST (3 AS" + " BIGINT)),",
            "ARRAY(NAMED_STRUCT('i', CAST (1 AS BIGINT))),",
            "NAMED_STRUCT('float_field', CAST(4.2 AS FLOAT), 'ts_field', CAST"
                + " (\"2019-03-18 01:23:45.678901\" AS TIMESTAMP)),",
            "MAP('mykey', MAP('subkey', 999))",
            "FROM (select '1') t"));
    // Read the data using the BQ SDK
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `${dataset}.%s`", TestUtils.ALL_TYPES_TABLE_NAME));
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
  }
}
