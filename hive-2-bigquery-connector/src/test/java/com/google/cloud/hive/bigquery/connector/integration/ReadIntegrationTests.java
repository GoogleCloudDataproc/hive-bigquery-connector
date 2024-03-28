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

import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import org.junit.jupiter.api.Test;

public class ReadIntegrationTests extends ReadIntegrationTestsBase {

  // Note: Other tests are inherited from the parent class

  /** Smoke test for UDFs that were added in Hive 2 */
  @Test
  public void testUDFWhereClauseSmokeForHive2() {
    System.getProperties().setProperty(HiveBigQueryConfig.FAIL_ON_UNSUPPORTED_UDFS, "true");
    initHive();
    createExternalTable(
        ALL_TYPES_TABLE_NAME, HIVE_ALL_TYPES_TABLE_DDL, BIGQUERY_ALL_TYPES_TABLE_DDL);
    String query =
        "select * from "
            + ALL_TYPES_TABLE_NAME
            + " where "
            + String.join(
                "\n OR ",
                "DAYOFWEEK(ts) = 2 AND QUARTER(ts) = 1 AND WEEKOFYEAR(day) = 4",
                "DATE(day + INTERVAL(5) DAY) > DATE('2001-09-05')",
                "str RLIKE '^([0-9]|[a-z]|[A-Z])'",
                "NULLIF(str, 'abcd') IS NULL",
                "LENGTH(str) > 2",
                "CHARACTER_LENGTH(str) > 2",
                "OCTET_LENGTH(str) > 2");
    runHiveQuery(query);
  }
}
