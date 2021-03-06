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

import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryClientModule;
import com.google.cloud.hive.bigquery.connector.Constants;
import com.google.cloud.hive.bigquery.connector.JobInfo;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConnectorModule;
import com.google.cloud.hive.bigquery.connector.utils.FileSystemUtils;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectOutputCommitter {

  private static final Logger LOG = LoggerFactory.getLogger(DirectOutputCommitter.class);

  /**
   * Commits the job by committing all open streams to BigQuery, where the individual tasks pushed
   * rows to. To find out which streams to commit, we read the stream reference files that the tasks
   * created in the job's work directory. The reference files essentially contain the stream names.
   */
  public static void commitJob(Configuration conf, JobInfo jobInfo) throws IOException {
    LOG.info("Committing BigQuery direct write job");
    List<String> streamFiles =
        FileSystemUtils.getFiles(
            conf,
            FileSystemUtils.getWorkDir(conf),
            DirectUtils.getTaskTempStreamFileNamePrefix(jobInfo.getTableId()),
            Constants.STREAM_FILE_EXTENSION);
    List<String> streamNames = new ArrayList<>();
    if (streamFiles.size() <= 0) {
      return;
    }
    // Extract the stream names from the stream reference files
    for (String streamFile : streamFiles) {
      Path path = new Path(streamFile);
      String streamName = FileSystemUtils.readFile(conf, path);
      streamNames.add(streamName);
    }
    Injector injector =
        Guice.createInjector(
            new BigQueryClientModule(),
            new HiveBigQueryConnectorModule(conf, jobInfo.getTableProperties()));
    BigQueryClient bqClient = injector.getInstance(BigQueryClient.class);
    BigQueryClientFactory bqClientFactory = injector.getInstance(BigQueryClientFactory.class);

    // Retrieve the BigQuery schema
    Schema bigQuerySchema = bqClient.getTable(jobInfo.getTableId()).getDefinition().getSchema();

    // Finally, make the new data available in the destination table by committing the streams
    DirectWriterContext writerContext =
        new DirectWriterContext(
            bqClient,
            bqClientFactory,
            jobInfo.getTableId(),
            jobInfo.getFinalTableId(),
            bigQuerySchema);
    writerContext.commit(streamNames);
  }

  public static void abortJob(Configuration conf, JobInfo jobInfo) {
    Injector injector =
        Guice.createInjector(
            new BigQueryClientModule(),
            new HiveBigQueryConnectorModule(conf, jobInfo.getTableProperties()));
    BigQueryClient bqClient = injector.getInstance(BigQueryClient.class);
    BigQueryClientFactory bqClientFactory = injector.getInstance(BigQueryClientFactory.class);

    // Retrieve the BigQuery schema
    Schema bigQuerySchema = bqClient.getTable(jobInfo.getTableId()).getDefinition().getSchema();
    DirectWriterContext writerContext =
        new DirectWriterContext(
            bqClient,
            bqClientFactory,
            jobInfo.getTableId(),
            jobInfo.getFinalTableId(),
            bigQuerySchema);
    writerContext.abort();
  }
}
