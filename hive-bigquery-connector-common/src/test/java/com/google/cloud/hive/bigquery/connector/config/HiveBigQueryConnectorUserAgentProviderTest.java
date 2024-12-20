/*
 * Copyright 2024 Google Inc. All Rights Reserved.
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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigquery.connector.common.UserAgentProvider;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class HiveBigQueryConnectorUserAgentProviderTest {

  @Test
  public void testBasicUserAgent() {
    Configuration conf = new Configuration();
    conf.set("hive.query.id", "abcd");
    Injector injector = Guice.createInjector(new HiveBigQueryConnectorModule(conf));
    UserAgentProvider provider = injector.getInstance(UserAgentProvider.class);
    assertThat(provider.getUserAgent()).matches("hive-bigquery-connector/test");
  }

  @Test
  public void testGPNUserAgent() {
    Configuration conf = new Configuration();
    conf.set("hive.query.id", "abcd");
    conf.set("GPN", "abcd");
    Injector injector = Guice.createInjector(new HiveBigQueryConnectorModule(conf));
    UserAgentProvider provider = injector.getInstance(UserAgentProvider.class);
    assertThat(provider.getUserAgent()).matches("hive-bigquery-connector/test \\(GPN:abcd\\)");
  }
}
