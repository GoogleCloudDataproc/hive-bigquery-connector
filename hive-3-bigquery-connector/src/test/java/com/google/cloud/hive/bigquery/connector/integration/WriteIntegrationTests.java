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

import static com.google.cloud.hive.bigquery.connector.TestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.common.collect.Streams;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class WriteIntegrationTests extends WriteIntegrationTestsBase {

  // Note: Other tests are from the parent class

  /** Check that we can write timestamps with time zone to BigQuery. */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_WRITE_METHOD)
  public void testWriteTimestampTz(String engine, String writeMethod) {
    System.getProperties().setProperty(HiveBigQueryConfig.WRITE_METHOD_KEY, writeMethod);
    initHive(engine, HiveBigQueryConfig.AVRO);
    // Create the BQ table
    createExternalTable(
        TIMESTAMP_TZ_TABLE_NAME, HIVE_TIMESTAMP_TZ_TABLE_DDL, BIGQUERY_TIMESTAMP_TZ_TABLE_DDL);
    // Insert data into the BQ table using Hive
    runHiveQuery(
        String.join(
            "\n",
            "INSERT INTO " + TIMESTAMP_TZ_TABLE_NAME + " SELECT",
            // (Pacific/Honolulu, -10:00)
            "CAST(\"2000-01-01 00:23:45.123456 Pacific/Honolulu\" AS TIMESTAMPLOCALTZ)"));
    // Read the data using the BQ SDK
    TableResult result =
        runBqQuery(String.format("SELECT * FROM `${dataset}.%s`", TIMESTAMP_TZ_TABLE_NAME));
    // Verify we get the expected values
    assertEquals(1, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    FieldValueList row = rows.get(0);
    assertEquals(1, row.size()); // Number of columns
    assertEquals(
        "2000-01-01T10:23:45.123456Z", row.get(0).getTimestampInstant().toString()); // 'Z' == UTC
  }
}
