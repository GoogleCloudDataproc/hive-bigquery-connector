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
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.connector.common.BigQueryCredentialsSupplier;
import com.google.cloud.bigquery.connector.common.BigQueryProxyConfig;
import com.google.cloud.bigquery.connector.common.BigQueryProxyTransporterBuilder;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.http.HttpTransportOptions;
import org.apache.hadoop.hive.metastore.api.Table;

public class BigQueryUtils {

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
