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
package com.google.cloud.hive.bigquery.connector.output;

import com.google.cloud.hive.bigquery.connector.JobDetails;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.output.direct.DirectOutputCommitter;
import com.google.cloud.hive.bigquery.connector.output.indirect.IndirectOutputCommitter;
import com.google.cloud.hive.bigquery.connector.utils.JobUtils;
import com.google.cloud.hive.bigquery.connector.utils.hive.HiveUtils;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutputCommitterUtils {
  private static final Logger LOG = LoggerFactory.getLogger(OutputCommitterUtils.class);

  public static void commitJob(Configuration conf, JobDetails jobDetails) throws IOException {
    HiveBigQueryConfig opts = HiveBigQueryConfig.from(conf, jobDetails.getTableProperties());
    String writeMethod = opts.getWriteMethod();
    // Pick the appropriate OutputCommitter (direct or indirect) based on the
    // configured write method
    try {
      if (writeMethod.equals(HiveBigQueryConfig.WRITE_METHOD_INDIRECT)) {
        IndirectOutputCommitter.commitJob(conf, jobDetails);
      } else {
        DirectOutputCommitter.commitJob(conf, jobDetails);
      }
    } finally {
      // deleteOnExit in case of other jobs using the same workdir
      JobUtils.cleanNotFail(
          () -> JobUtils.deleteJobTempOutput(conf, jobDetails),
          JobUtils.CleanMessage.DELETE_JOB_TEMPORARY_DIRECTORY);
    }
  }

  public static void abortJob(Configuration conf) throws IOException {
    Set<String> outputTables = getOutputTables(conf);
    LOG.info("aborting job {} with output tables {}", HiveUtils.getQueryId(conf), outputTables);
    for (String hmsDbTableName : outputTables) {
      JobDetails jobDetails;
      try {
        jobDetails = JobDetails.readJobDetailsFile(conf, hmsDbTableName);
      } catch (Exception e) {
        LOG.warn("JobDetails not found for table {}, skip it", hmsDbTableName);
        continue;
      }
      HiveBigQueryConfig opts = HiveBigQueryConfig.from(conf, jobDetails.getTableProperties());
      String writeMethod = opts.getWriteMethod();
      if (writeMethod.equals(HiveBigQueryConfig.WRITE_METHOD_DIRECT)) {
        DirectOutputCommitter.abortJob(conf, jobDetails);
      }
      JobUtils.deleteJobTempOutput(conf, jobDetails);
    }
  }

  public static Set<String> getOutputTables(Configuration conf) {
    String outputTables = conf.get(HiveBigQueryConfig.OUTPUT_TABLES_KEY);
    return Sets.newHashSet(HiveBigQueryConfig.TABLE_NAME_SPLITTER.split(outputTables));
  }
}
