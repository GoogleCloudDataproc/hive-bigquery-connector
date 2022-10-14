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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import java.util.List;
import org.junitpioneer.jupiter.cartesian.CartesianTest;

public class BigLakeIntegrationTests extends IntegrationTestsBase {

  @CartesianTest
  public void testReadBigLakeTable(
      @CartesianTest.Values(strings = {"mr", "tez"}) String engine,
      @CartesianTest.Values(strings = {HiveBigQueryConfig.ARROW, HiveBigQueryConfig.AVRO})
          String readDataFormat) {
    // Create BigLake table
    runBqQuery(BIGQUERY_BIGLAKE_TABLE_CREATE_QUERY);
    // Create Hive table
    initHive(engine, readDataFormat);
    runHiveScript(HIVE_BIGLAKE_TABLE_CREATE_QUERY);
    // Read data
    List<Object[]> rows = runHiveStatement(String.format("SELECT * FROM %s", BIGLAKE_TABLE_NAME));
    assertArrayEquals(
        new Object[] {
          new Object[] {1L, 2L, 3L},
          new Object[] {4L, 5L, 6L},
        },
        rows.toArray());
  }
}
