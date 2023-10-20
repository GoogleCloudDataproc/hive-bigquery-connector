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

import static com.google.cloud.hive.bigquery.connector.TestUtils.BIGQUERY_TEST_TABLE_DDL;
import static com.google.cloud.hive.bigquery.connector.TestUtils.HIVE_TEST_TABLE_DDL;
import static com.google.cloud.hive.bigquery.connector.TestUtils.TEST_TABLE_NAME;
import static com.google.cloud.hive.bigquery.connector.TestUtils.getProject;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

public abstract class SnapshotIntegrationTestsBase extends IntegrationTestsBase {

  @Test
  public void testSnapshot() {
    initHive();
    // Create the table in BigQuery
    createBqTable(TEST_TABLE_NAME, BIGQUERY_TEST_TABLE_DDL);
    runBqQuery(
        String.format(
            "INSERT `${dataset}.%s` VALUES (123, 'hello'), (999, 'abcd')", TEST_TABLE_NAME));
    // Create the snapshot in BigQuery
    String fullTableName = String.format("%s.%s.%s", getProject(), dataset, TEST_TABLE_NAME);
    String snapshot = String.format("%s.%s.%s_snapshot", getProject(), dataset, TEST_TABLE_NAME);
    runBqQuery(String.format("CREATE SNAPSHOT TABLE `%s` CLONE `%s`", snapshot, fullTableName));
    // Create the corresponding Hive table
    createExternalTable(TEST_TABLE_NAME + "_snapshot", HIVE_TEST_TABLE_DDL);
    // Read snapshot using Hive
    List<Object[]> rows =
        runHiveQuery(String.format("SELECT * FROM %s_snapshot ORDER BY NUMBER", TEST_TABLE_NAME));
    // Verify we get the expected rows
    assertArrayEquals(
        new Object[] {
          new Object[] {123L, "hello"},
          new Object[] {999L, "abcd"},
        },
        rows.toArray());
  }
}
