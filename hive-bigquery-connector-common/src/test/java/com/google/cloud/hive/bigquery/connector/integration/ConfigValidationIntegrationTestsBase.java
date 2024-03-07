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
package com.google.cloud.hive.bigquery.connector.integration;

import static com.google.cloud.hive.bigquery.connector.TestUtils.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import org.junit.jupiter.api.Test;

public abstract class ConfigValidationIntegrationTestsBase extends IntegrationTestsBase {

  /**
   * Check that the user didn't forget to provide all the required properties when creating a Hive
   * table.
   */
  @Test
  public void testMissingTableProperties() {
    initHive();
    Throwable exception =
        assertThrows(
            RuntimeException.class,
            () ->
                runHiveQuery(
                    String.join(
                        "\n",
                        "CREATE TABLE some_table (number BIGINT, text" + " STRING)",
                        "STORED BY"
                            + " 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'")));
    assertTrue(
        exception
            .getMessage()
            .contains("bq.table needs to be set in format of project.dataset.table"));
  }

  // ---------------------------------------------------------------------------------------------------

  /** Check that the user provides a GCS temporary path when using the "indirect" write method. */
  @Test
  public void testMissingGcsTempPath() {
    System.getProperties()
        .setProperty(HiveBigQueryConfig.WRITE_METHOD_KEY, HiveBigQueryConfig.WRITE_METHOD_INDIRECT);
    initHive(getDefaultExecutionEngine(), HiveBigQueryConfig.AVRO, "");
    createExternalTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL);
    Throwable exception =
        assertThrows(
            RuntimeException.class,
            () -> runHiveQuery("INSERT INTO " + TEST_TABLE_NAME + " VALUES (123, 'hello')"));
    assertTrue(
        exception
            .getMessage()
            .contains(
                "The 'bq.temp.gcs.path' property must be set when using the"
                    + " 'indirect' write method."));
  }

  /**
   * Check that the user has proper write permissions to the provided GCS temporary path when using
   * the "indirect" write method.
   */
  @Test
  public void testMissingBucketPermissions() {
    System.getProperties()
        .setProperty(HiveBigQueryConfig.WRITE_METHOD_KEY, HiveBigQueryConfig.WRITE_METHOD_INDIRECT);
    initHive(getDefaultExecutionEngine(), HiveBigQueryConfig.AVRO, NON_EXISTING_PATH);
    createExternalTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL, BIGQUERY_TEST_TABLE_DDL);
    Throwable exception =
        assertThrows(
            RuntimeException.class,
            () -> runHiveQuery("INSERT INTO " + TEST_TABLE_NAME + " VALUES (123, 'hello')"));
    String message = "bucket does not exist: " + NON_EXISTING_PATH;
    assertTrue(exception.getMessage().contains(message));
  }
}
