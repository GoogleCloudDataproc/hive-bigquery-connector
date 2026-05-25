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
package com.google.cloud.hive.bigquery.connector.utils.bigquery;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.http.HttpResponse;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Util to extract values from GCP environment */
public class GCPLabelUtils {

  public static final String BASE_URI = "http://metadata.google.internal/computeMetadata/v1";
  public static final String PROJECT_ID_ENDPOINT = "/project/project-id";
  public static final String CLUSTER_NAME_ENDPOINT = "/instance/attributes/dataproc-cluster-name";
  public static final String CLUSTER_UUID_ENDPOINT = "/instance/attributes/dataproc-cluster-uuid";
  public static final String DATAPROC_REGION_ENDPOINT = "/instance/attributes/dataproc-region";
  public static final String DATAPROC_CLASSPATH = "/usr/local/share/google/dataproc/lib";
  public static final String HIVE_QUERY_ID = "hive.query.id";
  public static final String HIVE_SESSION_ID = "hive.session.id";
  public static final String GOOGLE_METADATA_API = "google.metadata.api.base-url";
  private static final String METADATA_FLAVOUR = "Metadata-Flavor";
  private static final String GOOGLE = "Google";
  private static final Logger LOG = LoggerFactory.getLogger(GCPLabelUtils.class);

  private static Optional<Supplier<Map<String, String>>> hiveLabelsSupplier = Optional.empty();

  private static CloseableHttpClient createHttpClient() {
    RequestConfig requestConfig =
        RequestConfig.custom()
            .setConnectTimeout(1000)
            .setSocketTimeout(1000)
            .setConnectionRequestTimeout(100) // from pool
            .build();
    PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
    connectionManager.setDefaultMaxPerRoute(20);
    connectionManager.setMaxTotal(200);
    return HttpClients.custom()
        .setDefaultRequestConfig(requestConfig)
        .setConnectionManager(connectionManager)
        .build();
  }

  static boolean isDataprocRuntime(ImmutableMap<String, String> conf) {
    return conf.getOrDefault("yarn.application.classpath", "").contains(DATAPROC_CLASSPATH);
  }

  public static Map<String, String> getHiveLabels(ImmutableMap<String, String> conf) {
    Map<String, String> hiveLabels = new HashMap<>();
    getQueryId(conf).ifPresent(p -> hiveLabels.put("hive-query-id", p));
    getSessionId(conf).ifPresent(p -> hiveLabels.put("hive-session-id", p));
    synchronized (GCPLabelUtils.class) {
      if (!hiveLabelsSupplier.isPresent()) {
        hiveLabelsSupplier = Optional.of(Suppliers.memoize(() -> computeHiveLabels(conf)));
      }
    }
    hiveLabels.putAll(hiveLabelsSupplier.get().get());
    return hiveLabels;
  }

  @VisibleForTesting
  static void resetHiveLabelsCache() {
    hiveLabelsSupplier = Optional.empty();
  }

  private static Map<String, String> computeHiveLabels(ImmutableMap<String, String> conf) {
    Map<String, String> gcpLabels = new HashMap<>();
    if (isDataprocRuntime(conf)) {
      try (CloseableHttpClient httpClient = createHttpClient()) {
        getGCPProjectId(conf, httpClient).ifPresent(p -> gcpLabels.put("project-id", p));
        getDataprocRegion(conf, httpClient).ifPresent(p -> gcpLabels.put("region", p));
        getClusterName(conf, httpClient).ifPresent(p -> gcpLabels.put("cluster-name", p));
        getClusterUUID(conf, httpClient).ifPresent(p -> gcpLabels.put("cluster-uuid", p));
        gcpLabels.put("job-type", "hive_dataproc_job");
      } catch (IOException e) {
        LOG.warn("Failed to close HttpClient when computing hive labels", e);
      }
    }
    return gcpLabels;
  }

  @VisibleForTesting
  static Optional<String> getClusterName(
      ImmutableMap<String, String> conf, CloseableHttpClient httpClient) {
    return fetchGCPMetadata(CLUSTER_NAME_ENDPOINT, conf, httpClient);
  }

  @VisibleForTesting
  static Optional<String> getDataprocRegion(
      ImmutableMap<String, String> conf, CloseableHttpClient httpClient) {
    return fetchGCPMetadata(DATAPROC_REGION_ENDPOINT, conf, httpClient);
  }

  @VisibleForTesting
  static Optional<String> getGCPProjectId(
      ImmutableMap<String, String> conf, CloseableHttpClient httpClient) {
    return fetchGCPMetadata(PROJECT_ID_ENDPOINT, conf, httpClient)
        .map(b -> b.substring(b.lastIndexOf('/') + 1));
  }

  @VisibleForTesting
  static Optional<String> getQueryId(ImmutableMap<String, String> conf) {
    return Optional.ofNullable(conf.get(HIVE_QUERY_ID));
  }

  @VisibleForTesting
  static Optional<String> getSessionId(ImmutableMap<String, String> conf) {
    return Optional.ofNullable(conf.get(HIVE_SESSION_ID));
  }

  @VisibleForTesting
  static Optional<String> getClusterUUID(
      ImmutableMap<String, String> conf, CloseableHttpClient httpClient) {
    return fetchGCPMetadata(CLUSTER_UUID_ENDPOINT, conf, httpClient);
  }

  private static Optional<String> fetchGCPMetadata(
      String httpEndpoint, ImmutableMap<String, String> conf, CloseableHttpClient httpClient) {
    String baseUri = conf.getOrDefault(GOOGLE_METADATA_API, BASE_URI);
    String httpURI = baseUri + httpEndpoint;
    HttpGet httpGet = new HttpGet(httpURI);
    httpGet.addHeader(METADATA_FLAVOUR, GOOGLE);
    try {
      ResponseHandler<Optional<String>> handler =
          response -> {
            handleError(response);
            return Optional.of(EntityUtils.toString(response.getEntity(), UTF_8));
          };
      return httpClient.execute(httpGet, handler);
    } catch (IOException e) {
      LOG.warn("Failed to fetch GCP metadata from endpoint: {}", httpURI, e);
      return Optional.empty();
    }
  }

  private static void handleError(HttpResponse response) throws IOException {
    int statusCode = response.getStatusLine().getStatusCode();
    if (statusCode < 400 || statusCode >= 600) {
      return;
    }
    String body =
        response.getEntity() != null ? EntityUtils.toString(response.getEntity(), UTF_8) : "";
    throw new IOException(String.format("code: %d, response: %s", statusCode, body));
  }
}
