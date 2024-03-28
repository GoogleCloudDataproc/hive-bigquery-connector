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
package com.google.cloud.hive.bigquery.connector.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.hive.bigquery.connector.TestUtils;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.common.collect.Streams;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hive.common.util.HiveVersionInfo;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public abstract class PigIntegrationTestsBase extends IntegrationTestsBase {

  public static String readFileContents(Path directory, String filePrefix) throws IOException {
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, filePrefix + "*")) {
      for (Path entry : stream) {
        return Files.lines(entry).collect(Collectors.joining("\n"));
      }
    }
    return null;
  }

  /**
   * Converts timestamps to account for how HCatalog and Pig treat Hive timestamps in old versions
   * of Hive (1 and 2). See:
   * https://cwiki.apache.org/confluence/display/hive/hcatalog+loadstore#HCatalogLoadStore-DataTypeMappings
   */
  public static String convertTimestampForOldVersionsOfHive(String timestamp) {
    DateTimeFormatter pattern;
    if (timestamp.contains("T")) {
      pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS000");
    } else {
      pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    }
    LocalDateTime localDateTime = LocalDateTime.parse(timestamp, pattern);
    ZonedDateTime zonedSystemTime = ZonedDateTime.of(localDateTime, ZoneId.systemDefault());
    ZonedDateTime zonedUTC = zonedSystemTime.withZoneSameInstant(ZoneId.of("UTC"));
    return zonedUTC.format(DateTimeFormatter.ISO_INSTANT);
  }

  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_READ_FORMAT)
  public void testReadAllTypes(String engine, String readDataFormat) throws IOException {
    initHive(engine, readDataFormat);
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
            "cast(\"2000-01-01 00:23:45.123456\" as datetime),",
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
    // Define the read command in Pig
    Path outputDir = Files.createTempDirectory("pig-tests");
    PigServer pigServer = new PigServer(ExecType.LOCAL, hive.getHiveConf());
    pigServer.registerQuery(
        String.format(
            "result = LOAD '%s' USING org.apache.hive.hcatalog.pig.HCatLoader();",
            TestUtils.ALL_TYPES_TABLE_NAME));
    // Output the results
    pigServer.registerQuery(
        String.format(
            "STORE result INTO '%s/output' USING PigStorage('\t');", outputDir.toString()));
    // Check the output
    String[] contents = readFileContents(outputDir.resolve("output"), "part-").split("\t");
    assertEquals("11", contents[0]);
    assertEquals("22", contents[1]);
    assertEquals("33", contents[2]);
    assertEquals("44", contents[3]);
    assertEquals("true", contents[4]);
    assertEquals("fixed char", contents[5]);
    assertEquals("var char", contents[6]);
    assertEquals("string", contents[7]);
    assertTrue(contents[8].startsWith("2019-03-18T00:00:00.000"));
    assertTrue(contents[9].startsWith("2000-01-01T00:23:45.123"));
    assertEquals("bytes", contents[10]);
    assertEquals("2.0", contents[11]);
    assertEquals("4.2", contents[12]);
    assertEquals(
        "(-99999999999999999999999999999.999999999,99999999999999999999999999999.999999999,3.14,31415926535897932384626433832.795028841)",
        contents[13]);
    assertEquals("{(1),(2),(3)}", contents[14]);
    assertEquals("{(111),(222),(333)}", contents[15]);
    assertTrue(contents[16].startsWith("(4.2,2019-03-18T11:23:45.678"));
    assertEquals("[a_key#[a_subkey#888],b_key#[b_subkey#999]]", contents[17]);
  }

  @ParameterizedTest
  @MethodSource(IntegrationTestsBase.EXECUTION_ENGINE_WRITE_METHOD)
  public void testWriteAllTypes(String engine, String writeMethod) throws IOException {
    System.getProperties().setProperty(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    initHive(engine);
    createExternalTable(
        TestUtils.ALL_TYPES_TABLE_NAME,
        TestUtils.HIVE_ALL_TYPES_TABLE_DDL,
        TestUtils.BIGQUERY_ALL_TYPES_TABLE_DDL);
    // Create the input data
    Path inputDir = Files.createTempDirectory("pig-tests");
    Path inputFile = inputDir.resolve("input.txt");
    PigServer pigServer = new PigServer(ExecType.LOCAL, hive.getHiveConf());
    Files.write(
        inputFile,
        Collections.singletonList(
            String.join(
                "\t",
                "11",
                "22",
                "33",
                "44",
                "true",
                "fixed char",
                "var char",
                "string",
                "2019-03-18",
                "2000-01-01T00:23:45.123Z",
                "bytes",
                "2.0",
                "4.2",
                "(-99999999999999999999999999999.999999999,99999999999999999999999999999.999999999,3.14,31415926535897932384626433832.795028841)",
                "{(1),(2),(3)}",
                "{(1)}",
                "(4.2,2019-03-18T01:23:45.678Z)",
                // Note: HCatalog appears to not be able to load nested maps. For example, it
                // would fail with data shaped like "[a_key#[a_subkey#888]]". So we keep it simple
                // here and just test a map with a key and no value, as this is an issue with
                // HCatalog/Pig, not with the connector.
                "[mykey#]")),
        StandardCharsets.UTF_8);
    String inputType =
        String.join(
            ",",
            "tiny_int_val:int",
            "small_int_val:int",
            "int_val:int",
            "big_int_val:long",
            "bl:boolean",
            "fixed_char:chararray",
            "var_char:chararray",
            "str:chararray",
            "day:datetime",
            "ts:datetime",
            "bin:bytearray",
            "fl:float",
            "dbl:double",
            "nums:tuple(min:bigdecimal,max:bigdecimal,pi:bigdecimal,big_pi:bigdecimal)",
            "int_arr:bag{t:tuple(long)}",
            "int_struct_arr:bag{tuple(i:long)}",
            "mixed_struct:tuple(float_field:float,ts_field:datetime)",
            "mp:map[]");
    pigServer.registerQuery(
        String.format(
            "input_data = LOAD '%s' USING PigStorage('\t') AS (%s);", inputFile, inputType));
    // Write the data to BQ using Pig
    pigServer.registerQuery(
        String.format(
            "STORE input_data INTO '%s' USING org.apache.hive.hcatalog.pig.HCatStorer();",
            TestUtils.ALL_TYPES_TABLE_NAME));
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
    if (HiveVersionInfo.getVersion().charAt(0) <= '2') {
      assertEquals(
          "2000-01-01T00:23:45.123Z",
          convertTimestampForOldVersionsOfHive(row.get(9).getStringValue()));
    } else {
      assertEquals("2000-01-01T00:23:45.123000", row.get(9).getStringValue());
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
    // Mixed struct
    struct = row.get(16).getRecordValue();
    assertEquals(
        4.199999809265137,
        struct.get("float_field").getDoubleValue()); // TODO: Address discrepancy here
    if (HiveVersionInfo.getVersion().charAt(0) <= '2') {
      assertEquals(
          "2019-03-18T01:23:45.678Z",
          convertTimestampForOldVersionsOfHive(struct.get("ts_field").getStringValue()));
    } else {
      assertEquals("2019-03-18T01:23:45.678000", struct.get("ts_field").getStringValue());
    }
    // Check the Map type
    FieldValueList map = (FieldValueList) row.get(17).getRepeatedValue();
    assertEquals(1, map.size());
    FieldValueList entry = map.get(0).getRecordValue();
    assertEquals("mykey", entry.get(0).getStringValue());
    assertEquals(0, entry.get(1).getRepeatedValue().size());
    // Make sure things are correctly cleaned up
    checkThatWorkDirsHaveBeenCleaned();
  }
}
