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
import org.junit.jupiter.api.Test;

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

  /** Check that hive table is not created if BigQuery fails to create the table */
  @Test
  public void testCreateManagedTableFailInBQ() {
    initHive();
    // Try to create the managed table using Hive with invalid table name for BigQuery
    Throwable bqException =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                createHiveTable(
                    MANAGED_TEST_TABLE_NAME,
                    "invalid_$table:name",
                    HIVE_ALL_TYPES_TABLE_DDL,
                    false,
                    null,
                    null));
    assertTrue(bqException.getMessage().contains("Invalid table ID"));

    // Verify that table is not created in Hive.
    Throwable hiveException =
        assertThrows(
            IllegalArgumentException.class,
            () -> runHiveQuery("describe " + MANAGED_TEST_TABLE_NAME));
    assertTrue(hiveException.getMessage().contains("Table not found " + MANAGED_TEST_TABLE_NAME));
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
    runHiveQuery("DROP TABLE " + MANAGED_TEST_TABLE_NAME);
    // Check that the table in BigQuery is gone
    assertFalse(bQTableExists(dataset, MANAGED_TEST_TABLE_NAME));
  }

  // ---------------------------------------------------------------------------------------------------

  @Test
  public void testDropExternalTable() {
    initHive();
    createExternalTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL, BIGQUERY_TEST_TABLE_DDL);
    // Drop the external table in Hive
    runHiveQuery("DROP TABLE " + TEST_TABLE_NAME);
    // Check that the table still exists in BigQuery
    assertTrue(bQTableExists(dataset, TEST_TABLE_NAME));
  }
}
