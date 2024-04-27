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

import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestConstants.ACCEPTANCE_TEST_TIMEOUT_IN_SECONDS;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestConstants.CLEAN_UP_BQ;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestConstants.CLEAN_UP_CLUSTER;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestConstants.CLEAN_UP_GCS;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestConstants.CONNECTOR_INIT_ACTION_PATH;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestConstants.CONNECTOR_JAR_DIRECTORY;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestConstants.CONNECTOR_JAR_PREFIX;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestConstants.DATAPROC_ENDPOINT;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestConstants.REGION;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestUtils.createBqDataset;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestUtils.deleteBqDatasetAndTables;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestUtils.generateClusterName;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestUtils.generateTestId;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestUtils.readGcsFile;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestUtils.uploadConnectorInitAction;
import static com.google.cloud.hive.bigquery.connector.acceptance.AcceptanceTestUtils.uploadConnectorJar;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.hive.bigquery.connector.TestUtils;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
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
import test.hivebqcon.com.google.cloud.dataproc.v1.PigJob;
import test.hivebqcon.com.google.cloud.dataproc.v1.SoftwareConfig;

public abstract class DataprocAcceptanceTestBase {

  private AcceptanceTestContext context;

  protected DataprocAcceptanceTestBase(AcceptanceTestContext context) {
    this.context = context;
  }

  protected static AcceptanceTestContext setup(String dataprocImageVersion) throws Exception {
    String testId = generateTestId(dataprocImageVersion);
    String clusterName = generateClusterName(testId);
    String testBaseGcsDir = AcceptanceTestUtils.createTestBaseGcsDir(testId);
    String connectorJarUri = testBaseGcsDir + "/connector.jar";
    String connectorInitActionUri = testBaseGcsDir + "/connectors.sh";
    String bqProject = TestUtils.getProject();
    String bqDataset = "acceptance_dataset_" + testId.replace("-", "_");

    uploadConnectorJar(CONNECTOR_JAR_DIRECTORY, CONNECTOR_JAR_PREFIX, connectorJarUri);

    uploadConnectorInitAction(CONNECTOR_INIT_ACTION_PATH, connectorInitActionUri);

    createBqDataset(bqProject, bqDataset);

    createClusterIfNeeded(
        clusterName, dataprocImageVersion, connectorJarUri, connectorInitActionUri);

    AcceptanceTestContext testContext =
        new AcceptanceTestContext(
            testId,
            dataprocImageVersion,
            clusterName,
            testBaseGcsDir,
            connectorJarUri,
            connectorInitActionUri,
            bqProject,
            bqDataset);
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
      String connectorJarUri,
      String connectorInitActionUri)
      throws Exception {
    Cluster clusterSpec =
        createClusterSpec(
            clusterName, dataprocImageVersion, connectorJarUri, connectorInitActionUri);
    System.out.println("Cluster spec:\n" + clusterSpec);
    System.out.println("Creating cluster " + clusterName + " ...");
    cluster(client -> client.createClusterAsync(TestUtils.getProject(), REGION, clusterSpec).get());
  }

  protected static void deleteCluster(String clusterName) throws Exception {
    System.out.println("Deleting cluster " + clusterName + " ...");
    cluster(client -> client.deleteClusterAsync(TestUtils.getProject(), REGION, clusterName).get());
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
      String connectorJarUri,
      String connectorInitActionUri) {
    return Cluster.newBuilder()
        .setClusterName(clusterName)
        .setProjectId(TestUtils.getProject())
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
                    SoftwareConfig.newBuilder().setImageVersion(dataprocImageVersion)))
        .build();
  }

  private Job createAndRunHiveJob(String testName, String queryFile) throws Exception {
    Job job = createHiveJob(testName, queryFile);
    System.out.print("Running hive job:\n" + job);
    return runAndWait(testName, job);
  }

  private Job createAndRunPigJob(String testName, String queryFile) throws Exception {
    Job job = createPigJob(testName, queryFile);
    System.out.print("Running pig job:\n" + job);
    return runAndWait(testName, job);
  }

  private Map<String, String> getTestScriptVariables(String testName) {
    return ImmutableMap.<String, String>builder()
        .put("BQ_PROJECT", context.bqProject)
        .put("BQ_DATASET", context.bqDataset)
        .put("HIVE_TEST_TABLE", testName.replaceAll("-", "_") + "_table")
        .put("HIVE_OUTPUT_TABLE", testName.replaceAll("-", "_") + "_output")
        .put("HIVE_OUTPUT_DIR_URI", context.getOutputDirUri(testName))
        .build();
  }

  private String uploadQueryFile(String testName, String queryFile) throws Exception {
    String queryFileUri = context.getFileUri(testName, queryFile);
    AcceptanceTestUtils.uploadResourceToGcs("/acceptance/" + queryFile, queryFileUri, "text/x-sql");
    return queryFileUri;
  }

  private Job createHiveJob(String testName, String queryFile) throws Exception {
    String queryFileUri = uploadQueryFile(testName, queryFile);
    HiveJob.Builder hiveJobBuilder =
        HiveJob.newBuilder()
            .setQueryFileUri(queryFileUri)
            .putAllScriptVariables(getTestScriptVariables(testName));
    return Job.newBuilder()
        .setPlacement(JobPlacement.newBuilder().setClusterName(context.clusterId))
        .setHiveJob(hiveJobBuilder)
        .build();
  }

  private Job createPigJob(String testName, String queryFile) throws Exception {
    String queryFileUri = uploadQueryFile(testName, queryFile);
    PigJob.Builder pigJobBuilder =
        PigJob.newBuilder()
            .addJarFileUris("file:///usr/lib/hive/lib/datanucleus-core-5.2.2.jar")
            .setQueryFileUri(queryFileUri)
            .putAllScriptVariables(getTestScriptVariables(testName));
    return Job.newBuilder()
        .setPlacement(JobPlacement.newBuilder().setClusterName(context.clusterId))
        .setPigJob(pigJobBuilder)
        .build();
  }

  private Job runAndWait(String testName, Job job) throws Exception {
    try (JobControllerClient jobControllerClient =
        JobControllerClient.create(
            JobControllerSettings.newBuilder().setEndpoint(DATAPROC_ENDPOINT).build())) {
      Job request = jobControllerClient.submitJob(TestUtils.getProject(), REGION, job);
      String jobId = request.getReference().getJobId();
      System.err.printf("%s job ID: %s%n", testName, jobId);
      CompletableFuture<Job> finishedJobFuture =
          CompletableFuture.supplyAsync(
              () ->
                  waitForJobCompletion(jobControllerClient, TestUtils.getProject(), REGION, jobId));
      return finishedJobFuture.get(ACCEPTANCE_TEST_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
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

  void verifyJobSucceeded(Job job) throws Exception {
    String driverOutput =
        AcceptanceTestUtils.readGcsFile(job.getDriverControlFilesUri() + "driveroutput.000000000");
    System.out.println("Driver output: " + driverOutput);
    System.out.println("Job status: " + job.getStatus().getState());
    if (job.getStatus().getState() != JobStatus.State.DONE) {
      throw new AssertionError(job.getStatus().getDetails());
    }
  }

  void verifyGCSJobOutput(String outputDirUri, String expectedOutput) throws Exception {
    String output = readGcsFile(outputDirUri, "_0");
    assertThat(output.trim()).isEqualTo(expectedOutput);
  }

  @Test
  public void testWriteManagedTable() throws Exception {
    String testName = "test-hivebq-managed";
    String table = testName.replaceAll("-", "_") + "_table";
    Job job = createAndRunHiveJob(testName, "create_write_managed_table.sql");
    verifyJobSucceeded(job);
    // Wait a bit to give a chance for the committed writes to become readable
    Thread.sleep(60000); // 1 minute
    // Read the data using the BQ SDK
    TableResult result =
        TestUtils.getBigqueryClient()
            .query(
                String.format("SELECT * FROM `%s.%s` ORDER BY number", context.bqDataset, table));
    // Verify we get the expected values
    assertEquals(1, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    assertEquals(123L, rows.get(0).get(0).getLongValue());
    assertEquals("hello", rows.get(0).get(1).getStringValue());
  }

  @Test
  public void testCreateReadDropExternalTable() throws Exception {
    String testName = "test-hivebq-external";
    Job job = createAndRunHiveJob(testName, "create_read_drop_external_table.sql");
    verifyJobSucceeded(job);
    verifyGCSJobOutput(context.getOutputDirUri(testName), "king,1191");
  }

  @Test
  public void testPigMRMode() throws Exception {
    String testName = "test-pig";
    String sourceTable = testName.replaceAll("-", "_") + "_table";
    String destTable = testName.replaceAll("-", "_") + "_output";
    // Create the BQ tables
    TestUtils.getBigqueryClient()
        .query(
            String.format(
                "CREATE OR REPLACE TABLE `%s.%s` (%s)",
                context.bqDataset, sourceTable, TestUtils.BIGQUERY_TEST_TABLE_DDL));
    TestUtils.getBigqueryClient()
        .query(
            String.format(
                "CREATE OR REPLACE TABLE `%s.%s` (%s)",
                context.bqDataset, destTable, TestUtils.BIGQUERY_TEST_TABLE_DDL));
    TestUtils.getBigqueryClient()
        .query(
            String.format(
                "INSERT `%s.%s` VALUES (123, 'hello'), (789, 'abcd')",
                context.bqDataset, sourceTable));
    // Create the external test Hive tables
    Job hiveJob = createAndRunHiveJob(testName, "create_two_external_tables.sql");
    verifyJobSucceeded(hiveJob);
    // Run a pig Job
    Job pigJob = createAndRunPigJob(testName, "read_write.pig");
    verifyJobSucceeded(pigJob);
    // Wait a bit to give a chance for the committed writes to become readable
    Thread.sleep(60000); // 1 minute
    // Read the data using the BQ SDK
    TableResult result =
        TestUtils.getBigqueryClient()
            .query(
                String.format(
                    "SELECT * FROM `%s.%s` ORDER BY number", context.bqDataset, destTable));
    // Verify we get the expected values
    assertEquals(2, result.getTotalRows());
    List<FieldValueList> rows = Streams.stream(result.iterateAll()).collect(Collectors.toList());
    assertEquals(123L, rows.get(0).get(0).getLongValue());
    assertEquals("hello", rows.get(0).get(1).getStringValue());
    assertEquals(789L, rows.get(1).get(0).getLongValue());
    assertEquals("abcd", rows.get(1).get(1).getStringValue());
  }

  // TODO: Test Pig in Tez mode. Need a way to specify Tez as the Pig execution engine when
  //  submitting the Dataproc Pig job.
}
