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
package com.google.cloud.hive.bigquery.connector.output.direct;

import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryConnectorException;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsRequest.Builder;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectWriterContext {

  final Logger LOG = LoggerFactory.getLogger(DirectWriterContext.class);

  private final BigQueryClient bigQueryClient;

  private final TableId tableIdToWrite;
  private final TableId destinationTableId;
  private final String tablePathForBigQueryStorage;
  private final boolean deleteTableOnAbort;

  private final BigQueryWriteClient writeClient;

  public DirectWriterContext(
      BigQueryClient bigQueryClient,
      BigQueryClientFactory bigQueryWriteClientFactory,
      TableId tableId,
      TableId destinationTableId,
      boolean deleteTableOnAbort)
      throws IllegalArgumentException {
    this.bigQueryClient = bigQueryClient;
    this.tableIdToWrite = tableId;
    this.destinationTableId = destinationTableId;
    this.tablePathForBigQueryStorage =
        bigQueryClient.createTablePathForBigQueryStorage(tableIdToWrite);
    this.writeClient = bigQueryWriteClientFactory.getBigQueryWriteClient();
    this.deleteTableOnAbort = deleteTableOnAbort;
  }

  public void commit(List<String> streamNames) {
    Builder batchCommitWriteStreamsRequest =
        BatchCommitWriteStreamsRequest.newBuilder().setParent(tablePathForBigQueryStorage);
    batchCommitWriteStreamsRequest.addAllWriteStreams(streamNames);
    BatchCommitWriteStreamsResponse batchCommitWriteStreamsResponse =
        writeClient.batchCommitWriteStreams(batchCommitWriteStreamsRequest.build());

    // v1 does not have better error message, update when use v2
    if (!batchCommitWriteStreamsResponse.hasCommitTime()) {
      String streamErrors =
          batchCommitWriteStreamsResponse.getStreamErrorsList().stream()
              .map(se -> se.getErrorMessage())
              .collect(Collectors.joining(":"));
      throw new BigQueryConnectorException(
          "BigQuery writer failed to batch commit its BigQuery write-streams with StreamErrors: "
              + streamErrors);
    }

    LOG.info(
        "BigQuery writer has committed at time: {}",
        batchCommitWriteStreamsResponse.getCommitTime());

    // Special case for "INSERT OVERWRITE" statements: Overwrite the final
    // destination table with the contents of the temporary table.
    if (destinationTableId != null && !destinationTableId.equals(tableIdToWrite)) {
      LOG.info(
          "Loading from temporary table {} to destination table {}",
          tableIdToWrite,
          destinationTableId);
      Job overwriteJob =
          bigQueryClient.overwriteDestinationWithTemporary(tableIdToWrite, destinationTableId);
      bigQueryClient.waitForJob(overwriteJob);
    }
  }

  public void clean(boolean isAborting) {
    if (isAborting && deleteTableOnAbort
        || (destinationTableId != null && !destinationTableId.equals(tableIdToWrite))) {
      LOG.info("Deleting BigQuery table {}", tableIdToWrite);
      try {
        bigQueryClient.deleteTable(tableIdToWrite);
      } catch (Exception e) {
        LOG.warn("Failed cleaning up BigQuery table {}: {}", tableIdToWrite, e.getMessage());
      }
    }
  }
}
