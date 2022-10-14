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
import org.junitpioneer.jupiter.cartesian.CartesianTest;

public class ViewIntegrationTests extends IntegrationTestsBase {

  @CartesianTest
  public void testViewsDisabled(
      @CartesianTest.Values(strings = {"mr", "tez"}) String engine,
      @CartesianTest.Values(strings = {HiveBigQueryConfig.ARROW, HiveBigQueryConfig.AVRO})
          String readDataFormat) {
    // Disable views
    hive.setHiveConfValue(HiveBigQueryConfig.VIEWS_ENABLED_KEY, "false");
    // Create the table in BigQuery
    runBqQuery(BIGQUERY_TEST_TABLE_CREATE_QUERY);
    // Create the corresponding BigQuery view
    createOrReplaceBqView(dataset, TEST_TABLE_NAME, TEST_VIEW_NAME);
    // Create the corresponding Hive table
    initHive(engine, readDataFormat);
    runHiveScript(HIVE_TEST_VIEW_CREATE_QUERY);
    // Query the view
    Throwable exception =
        assertThrows(
            RuntimeException.class,
            () -> runHiveStatement(String.format("SELECT * FROM %s", TEST_VIEW_NAME)));
    assertTrue(exception.getMessage().contains("Views are not enabled"));
  }

  @CartesianTest
  public void testReadEmptyView(
      @CartesianTest.Values(strings = {"mr", "tez"}) String engine,
      @CartesianTest.Values(strings = {HiveBigQueryConfig.ARROW, HiveBigQueryConfig.AVRO})
          String readDataFormat) {
    // Enable views
    hive.setHiveConfValue(HiveBigQueryConfig.VIEWS_ENABLED_KEY, "true");
    // Create the table in BigQuery
    runBqQuery(BIGQUERY_TEST_TABLE_CREATE_QUERY);
    // Create the corresponding BigQuery view
    createOrReplaceBqView(dataset, TEST_TABLE_NAME, TEST_VIEW_NAME);
    // Create the corresponding Hive table
    initHive(engine, readDataFormat);
    runHiveScript(HIVE_TEST_VIEW_CREATE_QUERY);
    // Query the view
    List<Object[]> rows = runHiveStatement(String.format("SELECT * FROM %s", TEST_VIEW_NAME));
    assertThat(rows).isEmpty();
  }

  /** Test the WHERE clause */
  @CartesianTest
  public void testWhereClause(
      @CartesianTest.Values(strings = {"mr", "tez"}) String engine,
      @CartesianTest.Values(strings = {HiveBigQueryConfig.ARROW, HiveBigQueryConfig.AVRO})
          String readDataFormat) {
    // Enable views
    hive.setHiveConfValue(HiveBigQueryConfig.VIEWS_ENABLED_KEY, "true");
    // Create the table in BigQuery
    runBqQuery(BIGQUERY_TEST_TABLE_CREATE_QUERY);
    // Create the corresponding BigQuery view
    createOrReplaceBqView(dataset, TEST_TABLE_NAME, TEST_VIEW_NAME);
    // Create the corresponding Hive table
    initHive(engine, readDataFormat);
    runHiveScript(HIVE_TEST_VIEW_CREATE_QUERY);
    // Insert data into BQ using the BQ SDK
    runBqQuery(
        String.format(
            "INSERT `${dataset}.%s` VALUES (123, 'hello'), (999, 'abcd')", TEST_TABLE_NAME));
    // Make sure the initial data is there
    TableResult result = runBqQuery(String.format("SELECT * FROM `${dataset}.%s`", TEST_VIEW_NAME));
    assertEquals(2, result.getTotalRows());
    // Read filtered view using Hive
    List<Object[]> rows =
        runHiveStatement(String.format("SELECT * FROM %s WHERE number = 999", TEST_VIEW_NAME));
    // Verify we get the expected rows
    assertArrayEquals(
        new Object[] {
          new Object[] {999L, "abcd"},
        },
        rows.toArray());
    // TODO: Confirm that the predicate was in fact pushed down to BigQuery
  }
}
