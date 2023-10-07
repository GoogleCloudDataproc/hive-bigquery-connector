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

import static com.google.cloud.hive.bigquery.connector.TestUtils.HIVE_ALL_TYPES_TABLE_DDL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.api.client.http.HttpResponseException;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;

public abstract class ImpersonationIntegrationTestsBase extends IntegrationTestsBase {

  public static JsonObject getErrorJsonFromHttpResponseException(
      Throwable e, String stackTraceClassName, String stackTraceMethodName) {
    StackTraceElement[] stackTrace = e.getStackTrace();
    for (StackTraceElement element : stackTrace) {
      if (e instanceof HttpResponseException
          && element.getClassName().equals(stackTraceClassName)
          && element.getMethodName().equals(stackTraceMethodName)) {
        HttpResponseException httpError = (HttpResponseException) e;
        Gson gson = new Gson();
        TypeToken<JsonObject> token = new TypeToken<JsonObject>() {};
        return gson.fromJson(httpError.getContent(), token.getType());
      }
    }
    if (e.getCause() != null) {
      return getErrorJsonFromHttpResponseException(
          e.getCause(), stackTraceClassName, stackTraceMethodName);
    }
    return null;
  }

  @Test
  public void testGlobalImpersonation() {
    String sa = "abc@example.iam.gserviceaccount.com";
    hive.setHiveConfValue("bq.impersonation.service.account", sa);
    initHive();
    Throwable exception =
        assertThrows(
            RuntimeException.class, () -> createManagedTable("abcd", HIVE_ALL_TYPES_TABLE_DDL));
    JsonObject error =
        getErrorJsonFromHttpResponseException(
            exception, "com.google.auth.oauth2.ImpersonatedCredentials", "refreshAccessToken");
    assertEquals(
        error.getAsJsonObject("error").get("message").getAsString(),
        "Not found; Gaia id not found for email " + sa);
  }

  @Test
  public void testImpersonationForUser() {
    String user = "bob";
    String sa = "bob@example.iam.gserviceaccount.com";
    hive.setHiveConfValue("bq.impersonation.service.account.for.user." + user, sa);
    initHive();
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
    ugi.doAs(
        (java.security.PrivilegedAction<Void>)
            () -> {
              Throwable exception =
                  assertThrows(
                      RuntimeException.class,
                      () -> createManagedTable("abcd", HIVE_ALL_TYPES_TABLE_DDL));
              JsonObject error =
                  getErrorJsonFromHttpResponseException(
                      exception,
                      "com.google.auth.oauth2.ImpersonatedCredentials",
                      "refreshAccessToken");
              assertEquals(
                  error.getAsJsonObject("error").get("message").getAsString(),
                  "Not found; Gaia id not found for email " + sa);
              return null;
            });
  }

  @Test
  public void testImpersonationForGroup() {
    String user = "bob";
    String[] groups = new String[] {"datascience"};
    String sa = "datascience-team@example.iam.gserviceaccount.com";
    hive.setHiveConfValue("bq.impersonation.service.account.for.group." + "datascience", sa);
    initHive();
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(user, groups);
    ugi.doAs(
        (java.security.PrivilegedAction<Void>)
            () -> {
              Throwable exception =
                  assertThrows(
                      RuntimeException.class,
                      () -> createManagedTable("abcd", HIVE_ALL_TYPES_TABLE_DDL));
              JsonObject error =
                  getErrorJsonFromHttpResponseException(
                      exception,
                      "com.google.auth.oauth2.ImpersonatedCredentials",
                      "refreshAccessToken");
              assertEquals(
                  error.getAsJsonObject("error").get("message").getAsString(),
                  "Not found; Gaia id not found for email " + sa);
              return null;
            });
  }
}
