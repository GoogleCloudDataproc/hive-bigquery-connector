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

import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestConstants.CLEAN_UP_BQ;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestConstants.CLEAN_UP_CLUSTER;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestConstants.CLEAN_UP_GCS;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestConstants.CONNECTOR_INIT_ACTION_PATH;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestConstants.CONNECTOR_JAR_DIRECTORY;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestConstants.CONNECTOR_JAR_PREFIX;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestConstants.DATAPROC_ENDPOINT;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestConstants.PROJECT_ID;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestConstants.REGION;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestUtils.createBqDataset;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestUtils.deleteBqDatasetAndTables;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestUtils.generateClusterName;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestUtils.generateTestId;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestUtils.readGcsFile;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestUtils.uploadConnectorInitAction;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestUtils.uploadConnectorJar;
import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestUtils.ClusterProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.Test;
import test.hivebqcon.com.google.cloud.dataproc.v1.Cluster;
import test.hivebqcon.com.google.cloud.dataproc.v1.ClusterConfig;
import test.hivebqcon.com.google.cloud.dataproc.v1.ClusterControllerClient;
import test.hivebqcon.com.google.cloud.dataproc.v1.ClusterControllerSettings;
import test.hivebqcon.com.google.cloud.dataproc.v1.DiskConfig;
import test.hivebqcon.com.google.cloud.dataproc.v1.GceClusterConfig;
import test.hivebqcon.com.google.cloud.dataproc.v1.HiveJob;
import test.hivebqcon.com.google.cloud.dataproc.v1.InstanceGroupConfig;
import test.hivebqcon.com.google.cloud.dataproc.v1.Job;
import test.hivebqcon.com.google.cloud.dataproc.v1.JobControllerClient;
import test.hivebqcon.com.google.cloud.dataproc.v1.JobControllerSettings;
import test.hivebqcon.com.google.cloud.dataproc.v1.JobPlacement;
import test.hivebqcon.com.google.cloud.dataproc.v1.JobStatus;
import test.hivebqcon.com.google.cloud.dataproc.v1.NodeInitializationAction;
import test.hivebqcon.com.google.cloud.dataproc.v1.SoftwareConfig;

public class DataprocAcceptanceTestBase {

  protected static final ClusterProperty DISABLE_CONSCRYPT =
      ClusterProperty.of("dataproc:dataproc.conscrypt.provider.enable", "false", "nc");
  protected static final ImmutableList<ClusterProperty> DISABLE_CONSCRYPT_LIST =
      ImmutableList.<ClusterProperty>builder().add(DISABLE_CONSCRYPT).build();

  private AcceptanceTestContext context;

  protected DataprocAcceptanceTestBase(AcceptanceTestContext context) {
    this.context = context;
  }

  protected static AcceptanceTestContext setup(String dataprocImageVersion)
      throws Exception {
    return setup(dataprocImageVersion, Collections.emptyList());
  }

  protected static AcceptanceTestContext setup(
      String dataprocImageVersion,
      List<ClusterProperty> clusterProperties)
      throws Exception {
    String testId = generateTestId(dataprocImageVersion, clusterProperties);
    String clusterName = generateClusterName(testId);
    String testBaseGcsDir = AcceptanceTestUtils.createTestBaseGcsDir(testId);
    String connectorJarUri = testBaseGcsDir + "/connector.jar";
    String connectorInitActionUri = testBaseGcsDir + "/connectors.sh";
    Map<String, String> properties =
        clusterProperties.stream()
            .collect(Collectors.toMap(ClusterProperty::getKey, ClusterProperty::getValue));
    String bqProject = PROJECT_ID;
    String bqDataset = "hivebq_test_dataset_" + testId.replace("-", "_");
    String bqTable = "hivebq_test_table_" + testId.replace("-", "_");

    uploadConnectorJar(CONNECTOR_JAR_DIRECTORY, CONNECTOR_JAR_PREFIX, connectorJarUri);

    uploadConnectorInitAction(CONNECTOR_INIT_ACTION_PATH, connectorInitActionUri);

    createBqDataset(bqProject, bqDataset);

    createClusterIfNeeded(
        clusterName,
        dataprocImageVersion,
        testId,
        properties,
        connectorJarUri,
        connectorInitActionUri);

    AcceptanceTestContext testContext =
        new AcceptanceTestContext(
            testId,
            dataprocImageVersion,
            clusterName,
            testBaseGcsDir,
            connectorJarUri,
            connectorInitActionUri,
            bqProject,
            bqDataset,
            bqTable);
    System.out.print(testContext);

    return testContext;
  }

  protected static void tearDown(AcceptanceTestContext context) throws Exception {
    if (context == null) {
      System.out.println("Context is not initialized, skip cleanup.");
      return;
    }

    if (CLEAN_UP_CLUSTER) {
      deleteCluster(context.clusterId);
    } else {
      System.out.println("Skip deleting cluster: " + context.clusterId);
    }

    if (CLEAN_UP_GCS) {
      AcceptanceTestUtils.deleteGcsDir(context.testBaseGcsDir);
    } else {
      System.out.println("Skip deleting GCS dir: " + context.testBaseGcsDir);
    }

    if (CLEAN_UP_BQ) {
      deleteBqDatasetAndTables(context.bqDataset);
    } else {
      System.out.println("Skip deleting BQ dataset: " + context.bqDataset);
    }
  }

  @FunctionalInterface
  private interface ThrowingConsumer<T> {
    void accept(T t) throws Exception;
  }

  protected static void createClusterIfNeeded(
      String clusterName,
      String dataprocImageVersion,
      String testId,
      Map<String, String> properties,
      String connectorJarUri,
      String connectorInitActionUri)
      throws Exception {
    Cluster clusterSpec =
        createClusterSpec(
            clusterName, dataprocImageVersion, properties, connectorJarUri, connectorInitActionUri);
    System.out.println("Cluster spec:\n" + clusterSpec);
    System.out.println("Creating cluster " + clusterName + " ...");
    cluster(client -> client.createClusterAsync(PROJECT_ID, REGION, clusterSpec).get());
  }

  protected static void deleteCluster(String clusterName) throws Exception {
    System.out.println("Deleting cluster " + clusterName + " ...");
    cluster(client -> client.deleteClusterAsync(PROJECT_ID, REGION, clusterName).get());
  }

  private static void cluster(ThrowingConsumer<ClusterControllerClient> command) throws Exception {
    try (ClusterControllerClient clusterControllerClient =
        ClusterControllerClient.create(
            ClusterControllerSettings.newBuilder().setEndpoint(DATAPROC_ENDPOINT).build())) {
      command.accept(clusterControllerClient);
    }
  }

  private static Cluster createClusterSpec(
      String clusterName,
      String dataprocImageVersion,
      Map<String, String> properties,
      String connectorJarUri,
      String connectorInitActionUri) {
    return Cluster.newBuilder()
        .setClusterName(clusterName)
        .setProjectId(PROJECT_ID)
        .setConfig(
            ClusterConfig.newBuilder()
                .addInitializationActions(
                    NodeInitializationAction.newBuilder()
                        .setExecutableFile(String.format(connectorInitActionUri, REGION)))
                .setGceClusterConfig(
                    GceClusterConfig.newBuilder()
                        .setNetworkUri("default")
                        .setZoneUri(REGION + "-a")
                        .putMetadata("hive-bigquery-connector-url", connectorJarUri))
                .setMasterConfig(
                    InstanceGroupConfig.newBuilder()
                        .setNumInstances(1)
                        .setMachineTypeUri("n1-standard-4")
                        .setDiskConfig(
                            DiskConfig.newBuilder()
                                .setBootDiskType("pd-standard")
                                .setBootDiskSizeGb(300)
                                .setNumLocalSsds(0)))
                .setWorkerConfig(
                    InstanceGroupConfig.newBuilder()
                        .setNumInstances(2)
                        .setMachineTypeUri("n1-standard-4")
                        .setDiskConfig(
                            DiskConfig.newBuilder()
                                .setBootDiskType("pd-standard")
                                .setBootDiskSizeGb(300)
                                .setNumLocalSsds(0)))
                .setSoftwareConfig(
                    SoftwareConfig.newBuilder()
                        .setImageVersion(dataprocImageVersion)
                        .putAllProperties(properties)))
        .build();
  }

  private Job createAndRunHiveJob(
      String testName, String queryFile, String outputDirUri, Duration timeout) throws Exception {
    Job job = createHiveJob(testName, queryFile, outputDirUri);
    System.out.print("Running job:\n" + job);
    return runAndWait(testName, job, timeout);
  }

  private Job createHiveJob(String testName, String queryFile, String outputTableDirUri)
      throws Exception {
    String queryFileUri = context.getFileUri(testName, queryFile);
    AcceptanceTestUtils.uploadResourceToGcs("/acceptance/" + queryFile, queryFileUri, "text/x-sql");
    ImmutableMap<String, String> scriptVariables =
        ImmutableMap.<String, String>builder()
            .put("BQ_PROJECT", context.bqProject)
            .put("BQ_DATASET", context.bqDataset)
            .put("BQ_TABLE", context.bqTable)
            .put("HIVE_TEST_TABLE", testName.replaceAll("-", "_") + "_table")
            .put("HIVE_OUTPUT_TABLE", testName.replaceAll("-", "_") + "_output")
            .put("HIVE_OUTPUT_DIR_URI", outputTableDirUri)
            .build();
    HiveJob.Builder hiveJobBuilder =
        HiveJob.newBuilder().setQueryFileUri(queryFileUri).putAllScriptVariables(scriptVariables);
    return Job.newBuilder()
        .setPlacement(JobPlacement.newBuilder().setClusterName(context.clusterId))
        .setHiveJob(hiveJobBuilder)
        .build();
  }

  private Job runAndWait(String testName, Job job, Duration timeout) throws Exception {
    try (JobControllerClient jobControllerClient =
        JobControllerClient.create(
            JobControllerSettings.newBuilder().setEndpoint(DATAPROC_ENDPOINT).build())) {
      Job request = jobControllerClient.submitJob(PROJECT_ID, REGION, job);
      String jobId = request.getReference().getJobId();
      System.err.println(String.format("%s job ID: %s", testName, jobId));
      CompletableFuture<Job> finishedJobFuture =
          CompletableFuture.supplyAsync(
              () -> waitForJobCompletion(jobControllerClient, PROJECT_ID, REGION, jobId));
      Job jobInfo = finishedJobFuture.get(timeout.getSeconds(), TimeUnit.SECONDS);
      return jobInfo;
    }
  }

  Job waitForJobCompletion(
      JobControllerClient jobControllerClient, String projectId, String region, String jobId) {
    while (true) {
      // Poll the service periodically until the Job is in a finished state.
      Job jobInfo = jobControllerClient.getJob(projectId, region, jobId);
      switch (jobInfo.getStatus().getState()) {
        case DONE:
        case CANCELLED:
        case ERROR:
          return jobInfo;
        default:
          try {
            // Wait a second in between polling attempts.
            TimeUnit.SECONDS.sleep(1);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
      }
    }
  }

  void verifyJobSuceeded(Job job) throws Exception {
    String driverOutput =
        AcceptanceTestUtils.readGcsFile(job.getDriverControlFilesUri() + "driveroutput.000000000");
    System.out.println("Driver output: " + driverOutput);
    System.out.println("Job status: " + job.getStatus().getState());
    assertThat(job.getStatus().getState()).isEqualTo(JobStatus.State.DONE);
  }

  void verifyJobOutput(String outputDirUri, String expectedOutput) throws Exception {
    String output = readGcsFile(outputDirUri, "_0");
    assertThat(output.trim()).isEqualTo(expectedOutput);
  }

  @Test
  public void testHiveBq_CreateWriteReadTable_Success() throws Exception {
    String testName = "test-hivebq-cwr";
    String outputDirUri = context.getOutputDirUri(testName);

    Job result =
        createAndRunHiveJob(
            testName, "create_write_read_hivebq_table.sql", outputDirUri, Duration.ofSeconds(120));

    verifyJobSuceeded(result);
    verifyJobOutput(outputDirUri, "345,world");
  }
}
