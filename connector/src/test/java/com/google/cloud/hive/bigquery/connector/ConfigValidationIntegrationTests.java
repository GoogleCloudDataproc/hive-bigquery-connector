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

import static com.google.cloud.hive.bigquery.connector.TestUtils.HIVE_TEST_TABLE_DDL;
import static com.google.cloud.hive.bigquery.connector.TestUtils.TEST_TABLE_NAME;
import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import org.junit.jupiter.api.Test;

public class ConfigValidationIntegrationTests extends IntegrationTestsBase {

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
                runHiveScript(
                    String.join(
                        "\n",
                        "CREATE TABLE some_table (number BIGINT, text" + " STRING)",
                        "STORED BY"
                            + " 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler';")));
    assertTrue(
        exception
            .getMessage()
            .contains("The following table property(ies) must be provided: bq.dataset, bq.table"));
  }

  // ---------------------------------------------------------------------------------------------------

  /** Check that the user provides a GCS temporary path when using the "indirect" write method. */
  @Test
  public void testMissingGcsTempPath() {
    hive.setHiveConfValue(
        HiveBigQueryConfig.WRITE_METHOD_KEY, HiveBigQueryConfig.WRITE_METHOD_INDIRECT);
    initHive("mr", HiveBigQueryConfig.AVRO, "");
    createExternalTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL);
    Throwable exception =
        assertThrows(
            RuntimeException.class,
            () -> runHiveScript("INSERT INTO " + TEST_TABLE_NAME + " VALUES (123, 'hello')"));
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
    hive.setHiveConfValue(
        HiveBigQueryConfig.WRITE_METHOD_KEY, HiveBigQueryConfig.WRITE_METHOD_INDIRECT);
    initHive("mr", HiveBigQueryConfig.AVRO, "gs://random-bucket-abcdef-12345");
    createExternalTable(TEST_TABLE_NAME, HIVE_TEST_TABLE_DDL);
    Throwable exception =
        assertThrows(
            RuntimeException.class,
            () -> runHiveScript("INSERT INTO " + TEST_TABLE_NAME + " VALUES (123, 'hello')"));
    String message =
        "Cannot write to table 'default.test'. Does not have write access to the following GCS"
            + " path, or bucket does not exist: gs://random-bucket-abcdef-12345";
    assertTrue(exception.getMessage().contains(message));
  }
}
