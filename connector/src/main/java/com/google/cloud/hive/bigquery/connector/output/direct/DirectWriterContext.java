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
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryConnectorException;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import repackaged.by.hivebqconnector.com.google.common.base.Preconditions;

public class DirectWriterContext {

  final Logger LOG = LoggerFactory.getLogger(DirectWriterContext.class);

  private final BigQueryClient bigQueryClient;
  private final TableId tableIdToWrite;
  private final TableId destinationTableId;

  private final String tablePathForBigQueryStorage;
  private boolean deleteTableOnAbort;

  private final BigQueryWriteClient writeClient;

  public DirectWriterContext(
      BigQueryClient bigQueryClient,
      BigQueryClientFactory bigQueryWriteClientFactory,
      TableId tableId,
      TableId destinationTableId,
      Schema schema)
      throws IllegalArgumentException {
    this.bigQueryClient = bigQueryClient;
    this.tableIdToWrite = getOrCreateTable(tableId, schema);
    this.destinationTableId = destinationTableId;
    this.tablePathForBigQueryStorage =
        bigQueryClient.createTablePathForBigQueryStorage(tableIdToWrite);
    this.writeClient = bigQueryWriteClientFactory.getBigQueryWriteClient();
  }

  /**
   * This function determines whether the destination table exists: if it doesn't, we will create a
   * table and Hive will directly write to it.
   *
   * @param tableId the TableId, as was supplied by the user
   * @param bigQuerySchema the bigQuery schema
   * @return The TableId to which Hive will do the writing: whether that is the destination TableID
   *     or a temporary TableId.
   */
  private TableId getOrCreateTable(TableId tableId, com.google.cloud.bigquery.Schema bigQuerySchema)
      throws IllegalArgumentException {
    if (bigQueryClient.tableExists(tableId)) {
      TableInfo destinationTable = bigQueryClient.getTable(tableId);
      com.google.cloud.bigquery.Schema tableSchema = destinationTable.getDefinition().getSchema();
      Preconditions.checkArgument(
          BigQueryUtil.schemaEquals(tableSchema, bigQuerySchema, /* regardFieldOrder */ false),
          new BigQueryConnectorException.InvalidSchemaException(
              "Destination table's schema is not compatible with query's" + " schema"));
      deleteTableOnAbort = false;
      return destinationTable.getTableId();
    } else {
      deleteTableOnAbort = true;
      return bigQueryClient.createTable(tableId, bigQuerySchema).getTableId();
    }
  }

  public void commit(List<String> streamNames) {
    BatchCommitWriteStreamsRequest.Builder batchCommitWriteStreamsRequest =
        BatchCommitWriteStreamsRequest.newBuilder().setParent(tablePathForBigQueryStorage);
    for (String streamName : streamNames) {
      batchCommitWriteStreamsRequest.addWriteStreams(streamName);
    }
    BatchCommitWriteStreamsResponse batchCommitWriteStreamsResponse =
        writeClient.batchCommitWriteStreams(batchCommitWriteStreamsRequest.build());

    if (!batchCommitWriteStreamsResponse.hasCommitTime()) {
      throw new BigQueryConnectorException(
          "BigQuery writer failed to batch commit its BigQuery write-streams");
    }

    LOG.info(
        "BigQuery writer has committed at time: {}",
        batchCommitWriteStreamsResponse.getCommitTime());

    // Special case for "INSERT OVERWRITE" statements: Overwrite the final
    // destination table with the contents of the temporary table.
    if (destinationTableId != null && !destinationTableId.equals(tableIdToWrite)) {
      Job overwriteJob =
          bigQueryClient.overwriteDestinationWithTemporary(tableIdToWrite, destinationTableId);
      BigQueryClient.waitForJob(overwriteJob);
      Preconditions.checkState(
          bigQueryClient.deleteTable(tableIdToWrite),
          new BigQueryConnectorException(
              String.format("Could not delete temporary table %s from BigQuery", tableIdToWrite)));
    }
  }

  public void abort() {
    // Deletes the preliminary table we wrote to (if it exists):
    if (deleteTableOnAbort) {
      bigQueryClient.deleteTable(tableIdToWrite);
    }
  }
}
