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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.CartesianTest;

public class ManagedAndExternalHiveTableIntegrationTests extends IntegrationTestsBase {

  /** Check that creating a managed table using Hive also creates a table in BigQuery */
  @Test
  public void testCreateManagedTable() {
    initHive();
    // Make sure the managed table doesn't exist yet in BigQuery
    dropBqTableIfExists(dataset, MANAGED_TEST_TABLE_NAME);
    assertFalse(bQTableExists(dataset, MANAGED_TEST_TABLE_NAME));
    // Create the managed table using Hive
    createManagedTable(
        MANAGED_TEST_TABLE_NAME, HIVE_ALL_TYPES_TABLE_DDL, null, "A table with lots of types");
    // Create another BQ table with the same schema
    createBqTable(ALL_TYPES_TABLE_NAME, BIGQUERY_ALL_TYPES_TABLE_DDL, "A table with lots of types");
    // Make sure that the managed table was created in BQ
    // and that the two schemas are the same
    TableInfo managedTableInfo = getTableInfo(dataset, MANAGED_TEST_TABLE_NAME);
    TableInfo allTypesTableInfo = getTableInfo(dataset, ALL_TYPES_TABLE_NAME);
    assertEquals(managedTableInfo.getDescription(), allTypesTableInfo.getDescription());
    assertEquals(
        managedTableInfo.getDefinition().getSchema(),
        allTypesTableInfo.getDefinition().getSchema());
  }

  // ---------------------------------------------------------------------------------------------------

  /** Check that you can't create a managed table if the equivalent BigQuery table already exists */
  @Test
  public void testCreateManagedTableAlreadyExists() {
    initHive();
    // Create the table in BigQuery
    createBqTable(MANAGED_TEST_TABLE_NAME, BIGQUERY_ALL_TYPES_TABLE_DDL);
    // Try to create the managed table using Hive
    Throwable exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> createManagedTable(MANAGED_TEST_TABLE_NAME, HIVE_ALL_TYPES_TABLE_DDL));
    assertTrue(exception.getMessage().contains("BigQuery table already exists"));
  }

  // ---------------------------------------------------------------------------------------------------

  @Test
  public void testDropManagedTable() {
    initHive();
    // Make sure the managed table doesn't exist yet in BigQuery
    dropBqTableIfExists(dataset, MANAGED_TEST_TABLE_NAME);
    assertFalse(bQTableExists(dataset, MANAGED_TEST_TABLE_NAME));
    // Create the managed table using Hive
    createManagedTable(MANAGED_TEST_TABLE_NAME, HIVE_ALL_TYPES_TABLE_DDL);
    // Check that the table was created in BigQuery
    assertTrue(bQTableExists(dataset, MANAGED_TEST_TABLE_NAME));
    // Drop the managed table using hive
    runHiveScript("DROP TABLE " + MANAGED_TEST_TABLE_NAME);
    // Check that the table in BigQuery is gone
    assertFalse(bQTableExists(dataset, MANAGED_TEST_TABLE_NAME));
  }

  // ---------------------------------------------------------------------------------------------------

  @Test
  public void testDropExternalTable() {
    initHive();
    createExternalTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL, BIGQUERY_TEST_TABLE_DDL);
    // Drop the external table in Hive
    runHiveScript("DROP TABLE " + TEST_TABLE_NAME);
    // Check that the table still exists in BigQuery
    assertTrue(bQTableExists(dataset, TEST_TABLE_NAME));
  }

  // ---------------------------------------------------------------------------------------------------

  /** Create table as select (CTAS) */
  @CartesianTest
  public void testCTAS(
      @CartesianTest.Values(strings = {"mr", "tez"}) String engine,
      @CartesianTest.Values(strings = {HiveBigQueryConfig.ARROW, HiveBigQueryConfig.AVRO})
          String readDataFormat) {
    initHive(engine, readDataFormat);
    createExternalTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL, BIGQUERY_TEST_TABLE_DDL);
    // Insert data into the BQ tables using the BQ SDK
    runBqQuery(
        String.join(
            "\n",
            String.format("INSERT `${dataset}.%s` VALUES", TEST_TABLE_NAME),
            "(1, 'hello'), (2, 'bonjour'), (1, 'hola')"));
    // Create table as select (CTAS)
    runHiveScript(
        String.join(
            "\n",
            "CREATE TABLE ctas",
            "" + "STORED BY 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'",
            "TBLPROPERTIES (",
            "  'bq.project'='${project}',",
            "  'bq.dataset'='${dataset}',",
            "  'bq.table'='ctas'",
            ")",
            "AS SELECT * FROM " + TEST_TABLE_NAME));
  }
}
