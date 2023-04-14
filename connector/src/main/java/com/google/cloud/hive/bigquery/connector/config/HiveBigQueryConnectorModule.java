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
package com.google.cloud.hive.bigquery.connector.config;

import com.google.cloud.hive.bigquery.connector.utils.hive.HiveUtils;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import com.google.cloud.bigquery.connector.common.BigQueryConfig;
import com.google.cloud.bigquery.connector.common.UserAgentProvider;

/**
 * Class used to retrieve some bigquery-connector-common objects (e.g. the BigQuery client) when
 * using the Guice injector.
 */
public class HiveBigQueryConnectorModule implements Module {

  private final Configuration conf;
  private Map<String, String> tableParameters;

  public HiveBigQueryConnectorModule(Configuration conf) {
    this.conf = conf;
  }

  public HiveBigQueryConnectorModule(Configuration conf, Map<String, String> tableParameters) {
    this.conf = conf;
    this.tableParameters = tableParameters;
  }

  public HiveBigQueryConnectorModule(Configuration conf, Properties tableProperties) {
    this(conf, new HashMap<>());
    for (String key : tableProperties.stringPropertyNames()) {
      tableParameters.put(key, tableProperties.getProperty(key));
    }
  }

  @Override
  public void configure(Binder binder) {
    binder.bind(BigQueryConfig.class).toProvider(this::provideHiveBigQueryConfig);
  }

  @Singleton
  @Provides
  public HiveBigQueryConfig provideHiveBigQueryConfig() {
    return HiveBigQueryConfig.from(conf, tableParameters);
  }

  @Singleton
  @Provides
  public UserAgentProvider provideUserAgentProvider() {
    return new HiveBigQueryConnectorUserAgentProvider(HiveUtils.getHiveId(conf));
  }
}
