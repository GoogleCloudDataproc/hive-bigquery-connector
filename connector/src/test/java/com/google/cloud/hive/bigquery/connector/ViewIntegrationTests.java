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

import com.google.cloud.bigquery.TableResult;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class ViewIntegrationTests extends IntegrationTestsBase {

  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_READ_FORMAT)
  public void testViewsDisabled(String engine, String readDataFormat) {
    // Disable views
    hive.setHiveConfValue(HiveBigQueryConfig.VIEWS_ENABLED_KEY, "false");
    initHive(engine, readDataFormat);
    // Create the table in BigQuery
    createBqTable(TEST_TABLE_NAME, BIGQUERY_TEST_TABLE_DDL);
    // Create the corresponding BigQuery view
    createOrReplaceBqView(dataset, TEST_TABLE_NAME, TEST_VIEW_NAME);
    // Create the corresponding Hive table
    createExternalTable(TEST_VIEW_NAME, HIVE_TEST_VIEW_DDL);
    // Query the view
    Throwable exception =
        assertThrows(
            RuntimeException.class,
            () -> runHiveQuery(String.format("SELECT * FROM %s", TEST_VIEW_NAME)));
    assertTrue(exception.getMessage().contains("Views are not enabled"));
  }

  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_READ_FORMAT)
  public void testReadEmptyView(String engine, String readDataFormat) {
    // Enable views
    hive.setHiveConfValue(HiveBigQueryConfig.VIEWS_ENABLED_KEY, "true");
    initHive(engine, readDataFormat);
    // Create the table in BigQuery
    createBqTable(TEST_TABLE_NAME, BIGQUERY_TEST_TABLE_DDL);
    // Create the corresponding BigQuery view
    createOrReplaceBqView(dataset, TEST_TABLE_NAME, TEST_VIEW_NAME);
    // Create the corresponding Hive table
    createExternalTable(TEST_VIEW_NAME, HIVE_TEST_VIEW_DDL);
    // Query the view
    List<Object[]> rows = runHiveQuery(String.format("SELECT * FROM %s", TEST_VIEW_NAME));
    assertThat(rows).isEmpty();
  }

  /** Test the WHERE clause */
  @ParameterizedTest
  @MethodSource(EXECUTION_ENGINE_READ_FORMAT)
  public void testWhereClause(String engine, String readDataFormat) {
    // Enable views
    hive.setHiveConfValue(HiveBigQueryConfig.VIEWS_ENABLED_KEY, "true");
    initHive(engine, readDataFormat);
    // Create the table in BigQuery
    createBqTable(TEST_TABLE_NAME, BIGQUERY_TEST_TABLE_DDL);
    // Create the corresponding BigQuery view
    createOrReplaceBqView(dataset, TEST_TABLE_NAME, TEST_VIEW_NAME);
    // Create the corresponding Hive table
    createExternalTable(TEST_VIEW_NAME, HIVE_TEST_VIEW_DDL);
    // Insert data into BQ using the BQ SDK
    runBqQuery(
        String.format(
            "INSERT `${dataset}.%s` VALUES (123, 'hello'), (999, 'abcd')", TEST_TABLE_NAME));
    // Make sure the initial data is there
    TableResult result = runBqQuery(String.format("SELECT * FROM `${dataset}.%s`", TEST_VIEW_NAME));
    assertEquals(2, result.getTotalRows());
    // Read filtered view using Hive
    List<Object[]> rows =
        runHiveQuery(String.format("SELECT * FROM %s WHERE number = 999", TEST_VIEW_NAME));
    // Verify we get the expected rows
    assertArrayEquals(
        new Object[] {
          new Object[] {999L, "abcd"},
        },
        rows.toArray());
    // TODO: Confirm that the predicate was in fact pushed down to BigQuery
  }
}
