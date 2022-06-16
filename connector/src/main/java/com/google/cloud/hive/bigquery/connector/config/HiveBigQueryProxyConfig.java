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

import static repackaged.by.hivebqconnector.com.google.common.base.Optional.absent;

import com.google.cloud.bigquery.connector.common.BigQueryProxyConfig;
import java.io.Serializable;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import repackaged.by.hivebqconnector.com.google.common.base.Objects;
import repackaged.by.hivebqconnector.com.google.common.base.Optional;

/**
 * Class required to use the bigquery-connector-common library. It currently does not do anything.
 */
public class HiveBigQueryProxyConfig implements BigQueryProxyConfig, Serializable {

  private static final long serialVersionUID = 1L;

  private Optional<URI> proxyUri;
  private Optional<String> proxyUsername;
  private Optional<String> proxyPassword;

  public static HiveBigQueryProxyConfig from(Configuration conf) {
    HiveBigQueryProxyConfig config = new HiveBigQueryProxyConfig();
    config.proxyUri = absent();
    config.proxyUsername = absent();
    config.proxyPassword = absent();
    return config;
  }

  public java.util.Optional<URI> getProxyUri() {
    return proxyUri.toJavaUtil();
  }

  public java.util.Optional<String> getProxyUsername() {
    return proxyUsername.toJavaUtil();
  }

  public java.util.Optional<String> getProxyPassword() {
    return proxyPassword.toJavaUtil();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof HiveBigQueryProxyConfig)) {
      return false;
    }
    HiveBigQueryProxyConfig that = (HiveBigQueryProxyConfig) o;
    return Objects.equal(proxyUri, that.proxyUri)
        && Objects.equal(proxyUsername, that.proxyUsername)
        && Objects.equal(proxyPassword, that.proxyPassword);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(proxyUri, proxyUsername, proxyPassword);
  }
}
