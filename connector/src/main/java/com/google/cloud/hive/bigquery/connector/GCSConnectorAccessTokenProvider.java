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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.connector.common.BigQueryClientModule;
import com.google.cloud.bigquery.connector.common.BigQueryCredentialsSupplier;
import com.google.cloud.hadoop.util.AccessTokenProvider;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConnectorModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

/**
 * Simple AccessTokenProvider that delegates credentials retrieval to BigQueryCredentialsSupplier.
 */
public class GCSConnectorAccessTokenProvider implements AccessTokenProvider {

  Configuration conf;
  BigQueryCredentialsSupplier credentialsSupplier;
  private final static AccessToken EXPIRED_TOKEN = new AccessToken("", -1L);
  private AccessToken accessToken = EXPIRED_TOKEN;
  public static final String CLOUD_PLATFORM_SCOPE =
      "https://www.googleapis.com/auth/cloud-platform";

  @Override
  public AccessToken getAccessToken() {
    return accessToken;
  }

  @Override
  public void refresh() throws IOException {
    GoogleCredentials credentials = (GoogleCredentials) credentialsSupplier.getCredentials();
    com.google.auth.oauth2.AccessToken token = credentials.createScoped(CLOUD_PLATFORM_SCOPE).refreshAccessToken();
    this.accessToken = new AccessToken(token.getTokenValue(), token.getExpirationTime().getTime());
  }

  @Override
  public void setConf(Configuration configuration) {
    conf = configuration;
    Injector injector =
        Guice.createInjector(
            new BigQueryClientModule(), new HiveBigQueryConnectorModule(conf));
    credentialsSupplier =
        injector.getInstance(BigQueryCredentialsSupplier.class);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
