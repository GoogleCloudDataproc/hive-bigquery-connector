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
    runHiveScript(HIVE_MANAGED_TEST_TABLE_CREATE_QUERY);
    // Create another BQ table with the same schema
    runBqQuery(BIGQUERY_ALL_TYPES_TABLE_CREATE_QUERY);
    // Make sure that the managed table was created in BQ
    // and that the two schemas are the same
    TableInfo managedTableInfo = getTableInfo(dataset, MANAGED_TEST_TABLE_NAME);
    TableInfo allTypesTableInfo = getTableInfo(dataset, ALL_TYPES_TABLE_NAME);
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
    runBqQuery(BIGQUERY_MANAGED_TEST_TABLE_CREATE_QUERY);
    // Try to create the managed table using Hive
    Throwable exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> runHiveScript(HIVE_MANAGED_TEST_TABLE_CREATE_QUERY));
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
    runHiveScript(HIVE_MANAGED_TEST_TABLE_CREATE_QUERY);
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
    // Create the table in BigQuery
    runBqQuery(BIGQUERY_TEST_TABLE_CREATE_QUERY);
    // Create the corresponding external table in Hive
    runHiveScript(HIVE_TEST_TABLE_CREATE_QUERY);
    // Drop the external table
    runHiveScript("DROP TABLE " + TEST_TABLE_NAME);
    // Check that the table still exists in BigQuery
    assertTrue(bQTableExists(dataset, TEST_TABLE_NAME));
  }
}
