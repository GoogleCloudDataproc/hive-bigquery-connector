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

import com.google.cloud.bigquery.connector.common.UserAgentProvider;
import com.google.cloud.hive.bigquery.connector.utils.JobUtils;
import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

public class HiveBigQueryConnectorUserAgentProvider implements UserAgentProvider {

  private Optional<String> gpn;
  static String GCP_REGION_PART = getGcpRegion().map(region -> " region/" + region).orElse("");
  private static String JAVA_VERSION = System.getProperty("java.runtime.version");
  static String DATAPROC_IMAGE_PART =
      Optional.ofNullable(System.getenv("DATAPROC_IMAGE_VERSION"))
          .map(image -> " dataproc-image/" + image)
          .orElse("");

  public HiveBigQueryConnectorUserAgentProvider(Optional<String> gpn) {
    this.gpn = gpn;
  }

  static Optional<String> getGcpRegion() {
    RequestConfig config =
        RequestConfig.custom()
            .setConnectTimeout(100)
            .setConnectionRequestTimeout(100)
            .setSocketTimeout(100)
            .build();
    CloseableHttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(config).build();
    HttpGet httpGet =
        new HttpGet("http://metadata.google.internal/computeMetadata/v1/instance/zone");
    httpGet.addHeader("Metadata-Flavor", "Google");
    try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
      if (response.getStatusLine().getStatusCode() == 200) {
        String body =
            CharStreams.toString(
                new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8));
        return Optional.of(body.substring(body.lastIndexOf('/') + 1));
      } else {
        return Optional.empty();
      }
    } catch (Exception e) {
      return Optional.empty();
    } finally {
      try {
        Closeables.close(httpClient, true);
      } catch (IOException e) {
        // nothing to do
      }
    }
  }

  @Override
  public String getUserAgent() {
    StringBuilder userAgentBuilder = new StringBuilder();
    userAgentBuilder.append("hive-bigquery-connector/").append(JobUtils.CONNECTOR_VERSION);
    gpn.ifPresent(s -> userAgentBuilder.append(" (GPN:").append(s).append(")"));
    return userAgentBuilder.toString();
  }

  @Override
  /*
   * Creates JsonObject of the connector info as Json format is used at the receiver of this event.
   */
  public String getConnectorInfo() {
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("connectorVersion", JobUtils.CONNECTOR_VERSION);
    jsonObject.addProperty("dataprocImage", DATAPROC_IMAGE_PART);
    jsonObject.addProperty("gcpRegion", GCP_REGION_PART);
    jsonObject.addProperty("javaVersion", JAVA_VERSION);
    gpn.ifPresent(s -> jsonObject.addProperty("GPN", s));
    return jsonObject.toString();
  }
}
