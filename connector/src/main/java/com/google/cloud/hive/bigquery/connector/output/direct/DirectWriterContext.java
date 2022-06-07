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
  private final TableId destinationTableId;

  private final BigQueryTable tableToWrite;
  private final String tablePathForBigQueryStorage;

  private final BigQueryWriteClient writeClient;

  enum WritingMode {
    IGNORE_INPUTS,
    OVERWRITE,
    ALL_ELSE
  }

  private final WritingMode writingMode = WritingMode.ALL_ELSE;

  public DirectWriterContext(
      BigQueryClient bigQueryClient,
      BigQueryClientFactory bigQueryWriteClientFactory,
      TableId destinationTableId,
      Schema schema)
      throws IllegalArgumentException {
    this.bigQueryClient = bigQueryClient;
    this.destinationTableId = destinationTableId;
    this.tableToWrite = getOrCreateTable(destinationTableId, schema);
    this.tablePathForBigQueryStorage =
        bigQueryClient.createTablePathForBigQueryStorage(tableToWrite.getTableId());

    this.writeClient = bigQueryWriteClientFactory.getBigQueryWriteClient();
  }

  /**
   * This function determines whether the destination table exists: if it doesn't, we will create a
   * table and Hive will directly write to it.
   *
   * @param destinationTableId the TableId, as was supplied by the user
   * @param bigQuerySchema the bigQuery schema
   * @return The TableId to which Hive will do the writing: whether that is the destinationTableID
   *     or the temporaryTableId.
   */
  private BigQueryTable getOrCreateTable(
      TableId destinationTableId, com.google.cloud.bigquery.Schema bigQuerySchema)
      throws IllegalArgumentException {
    if (bigQueryClient.tableExists(destinationTableId)) {
      TableInfo destinationTable = bigQueryClient.getTable(destinationTableId);
      com.google.cloud.bigquery.Schema tableSchema = destinationTable.getDefinition().getSchema();
      Preconditions.checkArgument(
          BigQueryUtil.schemaEquals(tableSchema, bigQuerySchema, /* regardFieldOrder */ false),
          new BigQueryConnectorException.InvalidSchemaException(
              "Destination table's schema is not compatible with query's" + " schema"));
      return new BigQueryTable(destinationTable.getTableId(), false);
    } else {
      return new BigQueryTable(
          bigQueryClient.createTable(destinationTableId, bigQuerySchema).getTableId(), true);
    }
  }

  public void commit(List<String> streamNames) {
    if (writingMode.equals(WritingMode.IGNORE_INPUTS)) return;
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

    if (writingMode.equals(WritingMode.OVERWRITE)) {
      Job overwriteJob =
          bigQueryClient.overwriteDestinationWithTemporary(
              tableToWrite.getTableId(), destinationTableId);
      BigQueryClient.waitForJob(overwriteJob);
      Preconditions.checkState(
          bigQueryClient.deleteTable(tableToWrite.getTableId()),
          new BigQueryConnectorException(
              String.format("Could not delete temporary table %s from BigQuery", tableToWrite)));
    }
  }

  // Used for the getOrCreateTable output, to indicate if the table should be deleted on abort
  static class BigQueryTable {
    private final TableId tableId;
    private final boolean toDeleteOnAbort;

    public BigQueryTable(TableId tableId, boolean toDeleteOnAbort) {
      this.tableId = tableId;
      this.toDeleteOnAbort = toDeleteOnAbort;
    }

    public TableId getTableId() {
      return tableId;
    }

    public boolean toDeleteOnAbort() {
      return toDeleteOnAbort;
    }
  }
}
