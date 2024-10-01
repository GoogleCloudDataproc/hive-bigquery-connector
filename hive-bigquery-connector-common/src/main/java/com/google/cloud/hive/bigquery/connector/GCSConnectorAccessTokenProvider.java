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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Parameter;
import java.time.Instant;
import org.apache.hadoop.conf.Configuration;

/**
 * Simple AccessTokenProvider that delegates credentials retrieval to BigQueryCredentialsSupplier.
 */
public class GCSConnectorAccessTokenProvider implements AccessTokenProvider {

  // New versions (>=3.0) of the GCS connector used in Dataproc >=2.2 have changed the
  // signature of some constructors, so we use reflection to determine which constructor
  // to use and remain compatible with all versions of Dataproc.
  protected static boolean useNewGcsConnectorAPI;

  static {
    Constructor<?>[] constructors = AccessToken.class.getConstructors();
    for (Constructor<?> constructor : constructors) {
      Parameter[] parameters = constructor.getParameters();
      if (parameters.length == 2
          && parameters[0].getType() == String.class
          && parameters[1].getType() == Instant.class) {
        useNewGcsConnectorAPI = true;
        break;
      }
    }
  }

  public static AccessToken instantiateAccessToken(String stringArg, long timestamp) {
    try {
      if (useNewGcsConnectorAPI) {
        return AccessToken.class
            .getConstructor(String.class, Instant.class)
            .newInstance(stringArg, Instant.ofEpochMilli(timestamp));
      } else {
        return AccessToken.class
            .getConstructor(String.class, Long.class)
            .newInstance(stringArg, timestamp);
      }
    } catch (NoSuchMethodException
        | InvocationTargetException
        | IllegalAccessException
        | InstantiationException e) {
      throw new RuntimeException("Error instantiating AccessToken", e);
    }
  }

  Configuration conf;
  BigQueryCredentialsSupplier credentialsSupplier;
  private static final AccessToken EXPIRED_TOKEN = instantiateAccessToken("", -1L);
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
    com.google.auth.oauth2.AccessToken token =
        credentials.createScoped(CLOUD_PLATFORM_SCOPE).refreshAccessToken();
    this.accessToken =
        instantiateAccessToken(token.getTokenValue(), token.getExpirationTime().getTime());
  }

  @Override
  public void setConf(Configuration configuration) {
    conf = configuration;
    Injector injector =
        Guice.createInjector(new BigQueryClientModule(), new HiveBigQueryConnectorModule(conf));
    credentialsSupplier = injector.getInstance(BigQueryCredentialsSupplier.class);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
