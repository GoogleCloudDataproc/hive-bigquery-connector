/*
 * Copyright 2023 Google Inc. All Rights Reserved.
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
package com.google.cloud.hive.bigquery.connector.acceptance;

public class AcceptanceTestContext {
  final String testId;
  final String clusterId;
  final String testBaseGcsDir;
  final String connectorJarUri;
  final String connectorInitActionUri;
  final String bqProject;
  final String bqDataset;
  final String bqTable;

  public AcceptanceTestContext(
      String testId,
      String clusterId,
      String testBaseGcsDir,
      String connectorJarUri,
      String connectorInitActionUri,
      String bqProject,
      String bqDataset,
      String bqTable) {
    this.testId = testId;
    this.clusterId = clusterId;
    this.testBaseGcsDir = testBaseGcsDir;
    this.connectorJarUri = connectorJarUri;
    this.connectorInitActionUri = connectorInitActionUri;
    this.bqProject = bqProject;
    this.bqDataset = bqDataset;
    this.bqTable = bqTable;
  }

  public String getFileUri(String testName, String filename) {
    return testBaseGcsDir + "/" + testName + "/" + filename;
  }

  public String getOutputDirUri(String testName) {
    return testBaseGcsDir + "/" + testName + "/output";
  }

  @Override
  public String toString() {
    return new StringBuilder()
        .append("testId: " + testId + "\n")
        .append("clusterId: " + clusterId + "\n")
        .append("testBaseGcsDir: " + testBaseGcsDir + "\n")
        .append("connectorJarUri: " + connectorJarUri + "\n")
        .append("connectorInitActionUri: " + connectorInitActionUri + "\n")
        .append("projectId: " + bqProject + "\n")
        .append("bqDataset: " + bqDataset + "\n")
        .append("bqTable: " + bqTable + "\n")
        .toString();
  }
}
