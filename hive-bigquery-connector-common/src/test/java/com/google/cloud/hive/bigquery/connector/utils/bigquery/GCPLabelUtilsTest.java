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

import static org.junit.Assert.assertEquals;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;

public class GCPLabelUtilsTest {
  public static final Header METADATA_HEADER = new Header("Metadata-Flavor", "Google");
  private static final String TEST_SESSION_ID = "test-session";
  private static final String TEST_QUERY_ID = "test-query";
  private static final String TEST_CLUSTER_UUID = "1q2w3e4r5t6y7u8i";
  private static final String TEST_CLUSTER_NAME = "test-cluster";
  private static final String TEST_PROJECT_ID = "test-project";
  private static final String TEST_REGION = "us-central1";
  private static final String DATAPROC_LIB_PATH = "/usr/local/share/google/dataproc/lib";
  private ClientAndServer mockServer;
  private String mockBaseUrl;

  @Before
  public void setUp() {
    mockServer = ClientAndServer.startClientAndServer();
    mockBaseUrl = "http://localhost:" + mockServer.getPort();
    GCPLabelUtils.resetHiveLabelsCache();
  }

  @After
  public void tearDown() {
    mockServer.stop();
    GCPLabelUtils.resetHiveLabelsCache();
  }

  @Test
  public void testGetHiveLabelsNonDataprocReturnsOnlyQueryAndSessionId() {
    Map<String, String> labels = GCPLabelUtils.getHiveLabels(getConf("", false));

    // Should only contain query and session IDs, no GCP metadata
    assertEquals(2, labels.size());
    assertEquals(TEST_QUERY_ID, labels.get("hive-query-id"));
    assertEquals(TEST_SESSION_ID, labels.get("hive-session-id"));
  }

  @Test
  public void testGetHiveLabelsWithDataprocMetadataFetchFailure() {
    setupMockServerBaseSetup("", true);
    Map<String, String> labels = GCPLabelUtils.getHiveLabels(getConf("", true));

    // failures should happen silently and we should still get query and session IDs
    assertEquals(3, labels.size());
    assertEquals(TEST_QUERY_ID, labels.get("hive-query-id"));
    assertEquals(TEST_SESSION_ID, labels.get("hive-session-id"));
    assertEquals("hive_dataproc_job", labels.get("job-type"));
  }

  @Test
  public void testGetHiveLabelsDataprocRuntimeReturnsAllMetadata() {
    setupMockServerBaseSetup();
    Map<String, String> labels = GCPLabelUtils.getHiveLabels(getConf("", true));

    assertEquals(7, labels.size());
    assertEquals(TEST_QUERY_ID, labels.get("hive-query-id"));
    assertEquals(TEST_SESSION_ID, labels.get("hive-session-id"));
    assertEquals(TEST_PROJECT_ID, labels.get("project-id"));
    assertEquals(TEST_REGION, labels.get("region"));
    assertEquals(TEST_CLUSTER_NAME, labels.get("cluster-name"));
    assertEquals(TEST_CLUSTER_UUID, labels.get("cluster-uuid"));
    assertEquals("hive_dataproc_job", labels.get("job-type"));
  }

  @Test
  public void testGetHiveLabelsDataprocRuntimeCachesMetadata() {
    setupMockServerBaseSetup();
    Map<String, String> labelsFirst = GCPLabelUtils.getHiveLabels(getConf("", true));

    assertEquals(TEST_QUERY_ID, labelsFirst.get("hive-query-id"));
    assertEquals(TEST_SESSION_ID, labelsFirst.get("hive-session-id"));
    assertEquals(TEST_PROJECT_ID, labelsFirst.get("project-id"));
    assertEquals(TEST_REGION, labelsFirst.get("region"));
    assertEquals(TEST_CLUSTER_NAME, labelsFirst.get("cluster-name"));
    assertEquals(TEST_CLUSTER_UUID, labelsFirst.get("cluster-uuid"));

    // Change mock server to return different values
    String suffix = "-changed";
    setupMockServerBaseSetup(suffix, false);

    Map<String, String> labelsSecond = GCPLabelUtils.getHiveLabels(getConf(suffix, true));

    // Verify second call returns updated values for query and session IDs
    assertEquals(TEST_QUERY_ID + suffix, labelsSecond.get("hive-query-id"));
    assertEquals(TEST_SESSION_ID + suffix, labelsSecond.get("hive-session-id"));

    // Verify second call still returns original cached values for GCP metadata
    assertEquals(TEST_PROJECT_ID, labelsSecond.get("project-id"));
    assertEquals(TEST_REGION, labelsSecond.get("region"));
    assertEquals(TEST_CLUSTER_NAME, labelsSecond.get("cluster-name"));
    assertEquals(TEST_CLUSTER_UUID, labelsSecond.get("cluster-uuid"));
    assertEquals("hive_dataproc_job", labelsSecond.get("job-type"));
  }

  private ImmutableMap<String, String> getConf(String suffix, Boolean isDataprocRuntime) {
    return ImmutableMap.<String, String>builder()
        .put(GCPLabelUtils.HIVE_QUERY_ID, TEST_QUERY_ID + suffix)
        .put(GCPLabelUtils.HIVE_SESSION_ID, TEST_SESSION_ID + suffix)
        .put("yarn.application.classpath", isDataprocRuntime ? DATAPROC_LIB_PATH : "")
        .put(GCPLabelUtils.GOOGLE_METADATA_API, mockBaseUrl)
        .build();
  }

  private void setupMockServerBaseSetup() {
    setupMockServerBaseSetup("", false);
  }

  private void setupMockServerBaseSetup(String suffix, Boolean failEndpoints) {
    if (failEndpoints) {
      return404ForEndpoint(GCPLabelUtils.PROJECT_ID_ENDPOINT);
      return404ForEndpoint(GCPLabelUtils.DATAPROC_REGION_ENDPOINT);
      return404ForEndpoint(GCPLabelUtils.CLUSTER_UUID_ENDPOINT);
      return404ForEndpoint(GCPLabelUtils.CLUSTER_NAME_ENDPOINT);
      return;
    }
    return200ForEndpoint(GCPLabelUtils.PROJECT_ID_ENDPOINT, TEST_PROJECT_ID + suffix);
    return200ForEndpoint(GCPLabelUtils.DATAPROC_REGION_ENDPOINT, TEST_REGION + suffix);
    return200ForEndpoint(GCPLabelUtils.CLUSTER_UUID_ENDPOINT, TEST_CLUSTER_UUID + suffix);
    return200ForEndpoint(GCPLabelUtils.CLUSTER_NAME_ENDPOINT, TEST_CLUSTER_NAME + suffix);
  }

  private void return200ForEndpoint(String endpoint, String responseBody) {
    mockServer
        .when(request().withMethod("GET").withPath(endpoint).withHeader(METADATA_HEADER))
        .respond(response().withBody(responseBody));
  }

  private void return404ForEndpoint(String endpoint) {
    mockServer
        .when(request().withMethod("GET").withPath(endpoint))
        .respond(response().withStatusCode(404));
  }
}
