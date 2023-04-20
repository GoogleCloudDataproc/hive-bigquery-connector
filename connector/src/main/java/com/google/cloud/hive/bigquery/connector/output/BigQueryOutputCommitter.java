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
package com.google.cloud.hive.bigquery.connector.output;

import com.google.cloud.hive.bigquery.connector.JobDetails;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.output.direct.DirectOutputCommitter;
import com.google.cloud.hive.bigquery.connector.output.indirect.IndirectOutputCommitter;
import com.google.cloud.hive.bigquery.connector.utils.FileSystemUtils;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryOutputCommitter extends OutputCommitter {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryOutputCommitter.class);

  public static void commit(Configuration conf, JobDetails jobDetails) throws IOException {
    HiveBigQueryConfig opts = HiveBigQueryConfig.from(conf, jobDetails.getTableProperties());
    String writeMethod = opts.getWriteMethod();
    // Pick the appropriate OutputCommitter (direct or indirect) based on the
    // configured write method
    if (writeMethod.equals(HiveBigQueryConfig.WRITE_METHOD_INDIRECT)) {
      IndirectOutputCommitter.commitJob(conf, jobDetails);
    } else {
      DirectOutputCommitter.commitJob(conf, jobDetails);
    }
    FileSystemUtils.deleteWorkDirOnExit(conf, jobDetails.getHmsDbTableName());
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    JobConf jobConf = jobContext.getJobConf();
    Set<String> outputTables = getOutputTables(jobConf);
    LOG.info("Committing job {} with output tables {}", jobContext.getJobID(), outputTables);
    for (String hmsDbTableName : outputTables) {
      JobDetails jobDetails;
      try {
        jobDetails = JobDetails.readJobDetailsFile(jobConf, hmsDbTableName);
      } catch (Exception e) {
        // TO-DO: should we abort the job?
        LOG.warn("JobDetails not found for table {}, skip it", hmsDbTableName);
        continue;
      }
      commit(jobConf, jobDetails);
    }
    super.commitJob(jobContext);
  }

  @Override
  public void abortJob(JobContext jobContext, int status) throws IOException {
    JobConf jobConf = jobContext.getJobConf();
    Set<String> outputTables = getOutputTables(jobConf);
    LOG.info("aborting job {} with output tables {}", jobContext.getJobID(), outputTables);
    for (String hmsDbTableName : outputTables) {
      JobDetails jobDetails;
      try {
        jobDetails = JobDetails.readJobDetailsFile(jobConf, hmsDbTableName);
      } catch (Exception e) {
        LOG.warn("JobDetails not found for table {}, skip it", hmsDbTableName);
        continue;
      }
      DirectOutputCommitter.abortJob(jobConf, jobDetails);
      FileSystemUtils.deleteWorkDirOnExit(jobContext.getJobConf(), jobDetails.getHmsDbTableName());
    }
    super.abortJob(jobContext, status);
  }

  @Override
  public void setupJob(JobContext jobContext) throws IOException {
    // Do nothing
  }

  @Override
  public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {
    // Do nothing
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
    return false;
  }

  @Override
  public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {
    // Do nothing
  }

  @Override
  public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
    // Do nothing
  }

  private Set<String> getOutputTables(JobConf jobConf) {
    String outputTables = jobConf.get(HiveBigQueryConfig.OUTPUT_TABLES_KEY);
    return Sets.newHashSet(HiveBigQueryConfig.TABLE_NAME_SPLITTER.split(outputTables));
  }
}
