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
package com.google.cloud.hive.bigquery.connector.output.indirect;

import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.JobInfo.WriteDisposition;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientModule;
import com.google.cloud.hive.bigquery.connector.Constants;
import com.google.cloud.hive.bigquery.connector.JobInfo;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConnectorModule;
import com.google.cloud.hive.bigquery.connector.utils.FileSystemUtils;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndirectOutputCommitter {

  private static final Logger LOG = LoggerFactory.getLogger(IndirectOutputCommitter.class);

  /**
   * Commits the job by loading all the Avro files (which were created by the individual tasks) from
   * GCS to BigQuery.
   */
  public static void commitJob(Configuration conf, JobInfo jobInfo) throws IOException {
    LOG.info("Committing BigQuery load job");
    // Retrieve the list of Avro files from GCS
    List<String> avroFiles =
        FileSystemUtils.getFiles(
            conf,
            IndirectUtils.getGcsTempDir(conf, jobInfo.getGcsTempPath()),
            IndirectUtils.getTaskTempAvroFileNamePrefix(jobInfo.getTableId()),
            Constants.LOAD_FILE_EXTENSION);
    if (avroFiles.size() > 0) {
      Injector injector =
          Guice.createInjector(
              new BigQueryClientModule(),
              new HiveBigQueryConnectorModule(conf, jobInfo.getTableProperties()));
      BigQueryClient bqClient = injector.getInstance(BigQueryClient.class);
      HiveBigQueryConfig opts = injector.getInstance(HiveBigQueryConfig.class);
      FormatOptions formatOptions = FormatOptions.avro();
      WriteDisposition writeDisposition =
          jobInfo.isOverwrite() ? WriteDisposition.WRITE_TRUNCATE : WriteDisposition.WRITE_APPEND;
      try {
        // Load the Avro files into BigQuery
        bqClient.loadDataIntoTable(opts, avroFiles, formatOptions, writeDisposition);
      }
      finally {
        // Delete all the Avro files from GCS
        IndirectUtils.deleteGcsTempDir(conf, jobInfo.getGcsTempPath());
      }
    }
  }
}
