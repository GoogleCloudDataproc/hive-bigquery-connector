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

import com.google.api.gax.rpc.HeaderProvider;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConnectorModule;
import com.google.cloud.hive.bigquery.connector.utils.FileSystemUtils;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptID;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryClientModule;
import com.google.cloud.bigquery.connector.common.BigQueryCredentialsSupplier;
import com.google.cloud.bigquery.connector.common.BigQueryDirectDataWriterHelper;

public class DirectUtils {

  /** Return the name prefix for the temp stream file. */
  public static String getTaskTempStreamFileNamePrefix(TableId tableId) {
    return String.format(
            "%s_%s_%s",
            tableId.getProject(), tableId.getDataset(), tableId.getTable().replace("$", "__"))
        .replace(":", "__");
  }

  /**
   * Returns the name of the stream reference file for the given task. The stream reference files
   * will essentially contain the name of the stream that the task writes data to.
   */
  public static Path getTaskTempStreamFile(
      Configuration conf,
      String hmsDbTableName,
      TableId bigQueryTableId,
      TaskAttemptID taskAttemptID) {
    Path hmsTablePath = new Path(FileSystemUtils.getWorkDir(conf), hmsDbTableName);
    return new Path(
        hmsTablePath,
        String.format(
            "%s_%s.%s",
            getTaskTempStreamFileNamePrefix(bigQueryTableId),
            taskAttemptID.getTaskID(),
            HiveBigQueryConfig.STREAM_FILE_EXTENSION));
  }

  /**
   * Instantiates a BigQueryDirectDataWriterHelper object from the bigquery-connector-common
   * library. That helper is responsible for handling all the interactions with the BQ Storage Write
   * API.
   */
  public static BigQueryDirectDataWriterHelper createStreamWriter(
      JobConf jobConf, TableId tableId, Properties tableProperties, ProtoSchema schema) {
    Injector injector =
        Guice.createInjector(
            new BigQueryClientModule(), new HiveBigQueryConnectorModule(jobConf, tableProperties));
    HeaderProvider headerProvider = injector.getInstance(HeaderProvider.class);
    BigQueryCredentialsSupplier credentialsSupplier =
        injector.getInstance(BigQueryCredentialsSupplier.class);
    HiveBigQueryConfig opts = injector.getInstance(HiveBigQueryConfig.class);
    String tablePath =
        String.format(
            "projects/%s/datasets/%s/tables/%s",
            tableId.getProject(), tableId.getDataset(), tableId.getTable());
    BigQueryClientFactory writeClientFactory =
        new BigQueryClientFactory(credentialsSupplier, headerProvider, opts);
    return new BigQueryDirectDataWriterHelper(
        writeClientFactory,
        tablePath,
        schema,
        opts.getBigQueryClientRetrySettings(),
        opts.getTraceId());
  }
}
