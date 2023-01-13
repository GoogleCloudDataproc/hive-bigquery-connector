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

import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.connector.common.*;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.hive.bigquery.connector.PartitionSpec;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import repackaged.by.hivebqconnector.com.google.common.base.Preconditions;

public class DirectWriterContext {

  final Logger LOG = LoggerFactory.getLogger(DirectWriterContext.class);

  private final BigQueryClient bigQueryClient;
  private final BigQuery bq; // TODO: Remove

  private final boolean isOverwrite;
  private final TableId tableIdToWrite;
  private final TableId destinationTableId;
  private final boolean enableModeCheckForSchemaFields;
  private final PartitionSpec partitionSpec;

  private final String tablePathForBigQueryStorage;
  private boolean deleteTableOnAbort;

  private final BigQueryWriteClient writeClient;

  public DirectWriterContext(
      BigQuery bq,
      BigQueryClient bigQueryClient,
      BigQueryClientFactory bigQueryWriteClientFactory,
      boolean isOverwrite,
      TableId tableId,
      TableId destinationTableId,
      PartitionSpec partitionSpec,
      Schema schema,
      boolean enableModeCheckForSchemaFields)
      throws IllegalArgumentException {
    this.bq = bq;
    this.bigQueryClient = bigQueryClient;
    this.isOverwrite = isOverwrite;
    this.tableIdToWrite = getOrCreateTable(tableId, schema);
    this.destinationTableId = destinationTableId;
    this.partitionSpec = partitionSpec;
    this.tablePathForBigQueryStorage =
        bigQueryClient.createTablePathForBigQueryStorage(tableIdToWrite);
    this.writeClient = bigQueryWriteClientFactory.getBigQueryWriteClient();
    this.enableModeCheckForSchemaFields = enableModeCheckForSchemaFields;
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
          BigQueryUtil.schemaEquals(
              tableSchema,
              bigQuerySchema, /* regardFieldOrder */
              false,
              enableModeCheckForSchemaFields),
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
    if (isOverwrite) {
      if (partitionSpec == null) { // Overwrite entire table
        Job overwriteJob =
            bigQueryClient.overwriteDestinationWithTemporary(tableIdToWrite, destinationTableId);
        BigQueryClient.waitForJob(overwriteJob);
        Preconditions.checkState(
            bigQueryClient.deleteTable(tableIdToWrite),
            new BigQueryConnectorException(
                String.format(
                    "Could not delete temporary table %s from BigQuery", tableIdToWrite)));
      } else { // Overwrite partition
        String queryFormat =
            String.join(
                "\n",
                "MERGE `%s`",
                "USING `%s`",
                "ON FALSE",
                "WHEN NOT MATCHED THEN INSERT ROW",
                "WHEN NOT MATCHED BY SOURCE AND (%s = '%s') THEN DELETE");
        String destinationTableName = BigQueryClient.fullTableName(destinationTableId);
        String temporaryTableName = BigQueryClient.fullTableName(tableIdToWrite);
        String query =
            String.format(
                queryFormat,
                destinationTableName,
                temporaryTableName,
                partitionSpec.getName(),
                partitionSpec.getStaticValue());
        QueryJobConfiguration queryConfig =
            QueryJobConfiguration.newBuilder(query).setUseLegacySql(false).build();
        Job overwriteJob = bq.create(JobInfo.newBuilder(queryConfig).build());
        BigQueryClient.waitForJob(overwriteJob);
        Preconditions.checkState(
            bigQueryClient.deleteTable(tableIdToWrite),
            new BigQueryConnectorException(
                String.format(
                    "Could not delete temporary table %s from BigQuery", tableIdToWrite)));
      }
    }
  }

  public void abort() {
    // Deletes the preliminary table we wrote to (if it exists):
    if (deleteTableOnAbort) {
      bigQueryClient.deleteTable(tableIdToWrite);
    }
  }
}
