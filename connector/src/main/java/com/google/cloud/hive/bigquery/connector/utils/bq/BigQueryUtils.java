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
package com.google.cloud.hive.bigquery.connector.utils.bq;

import com.google.api.gax.rpc.HeaderProvider;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.connector.common.BigQueryCredentialsSupplier;
import com.google.cloud.bigquery.connector.common.BigQueryProxyConfig;
import com.google.cloud.bigquery.connector.common.BigQueryProxyTransporterBuilder;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.http.HttpTransportOptions;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.metastore.api.Table;
import repackaged.by.hivebqconnector.com.google.gson.Gson;
import repackaged.by.hivebqconnector.com.google.gson.JsonArray;
import repackaged.by.hivebqconnector.com.google.gson.JsonElement;
import repackaged.by.hivebqconnector.com.google.gson.JsonObject;

public class BigQueryUtils {

  public static String exportSchemaToJSON(Schema schema) {
    Gson gson = new Gson();
    return gson.toJson(schema);
  }

  public static FieldList loadFieldsFromJSON(JsonArray jsonFields) {
    List<Field> fields = new ArrayList<>();
    for (int i = 0; i < jsonFields.size(); i++) {
      JsonObject fieldJson = jsonFields.get(i).getAsJsonObject();
      String name = fieldJson.get("name").getAsString();
      String type = fieldJson.get("type").getAsJsonObject().get("constant").getAsString();
      Field.Builder fieldBuilder;
      if (type.equals("RECORD")) {
        FieldList subFields = loadFieldsFromJSON(fieldJson.get("subFields").getAsJsonArray());
        fieldBuilder = Field.newBuilder(name, LegacySQLTypeName.valueOf(type), subFields);
      } else {
        fieldBuilder = Field.newBuilder(name, LegacySQLTypeName.valueOf(type));
      }
      JsonElement mode = fieldJson.get("mode");
      if (mode != null) {
        fieldBuilder.setMode(Field.Mode.valueOf(mode.getAsString()));
      }
      fields.add(fieldBuilder.build());
    }
    return FieldList.of(fields);
  }

  public static Schema loadSchemaFromJSON(String json) {
    Gson gson = new Gson();
    JsonArray jsonArray = gson.fromJson(json, JsonObject.class).getAsJsonArray("fields");
    FieldList fields = loadFieldsFromJSON(jsonArray);
    return Schema.of(fields);
  }

  /**
   * Returns a BigQuery service object. We need this instead of the BigQueryClient class from the
   * bigquery-connector-common library because that class doesn't have a `create(TableInfo)` method.
   * See more about this in {@link
   * com.google.cloud.hive.bigquery.connector.BigQueryMetaHook#commitCreateTable(Table)}
   */
  public static BigQuery getBigQueryService(
      HiveBigQueryConfig config,
      HeaderProvider headerProvider,
      BigQueryCredentialsSupplier bigQueryCredentialsSupplier) {
    BigQueryOptions.Builder options =
        BigQueryOptions.newBuilder()
            .setHeaderProvider(headerProvider)
            .setProjectId(config.getParentProjectId())
            .setCredentials(bigQueryCredentialsSupplier.getCredentials())
            .setRetrySettings(config.getBigQueryClientRetrySettings());

    HttpTransportOptions.Builder httpTransportOptionsBuilder =
        HttpTransportOptions.newBuilder()
            .setConnectTimeout(config.getBigQueryClientConnectTimeout())
            .setReadTimeout(config.getBigQueryClientReadTimeout());
    BigQueryProxyConfig proxyConfig = config.getBigQueryProxyConfig();
    if (proxyConfig.getProxyUri().isPresent()) {
      httpTransportOptionsBuilder.setHttpTransportFactory(
          BigQueryProxyTransporterBuilder.createHttpTransportFactory(
              proxyConfig.getProxyUri(),
              proxyConfig.getProxyUsername(),
              proxyConfig.getProxyPassword()));
    }

    options.setTransportOptions(httpTransportOptionsBuilder.build());
    return options.build().getService();
  }
}
