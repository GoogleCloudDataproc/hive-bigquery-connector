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

import com.google.cloud.hive.bigquery.connector.BigQueryMetaHook;
import com.google.cloud.hive.bigquery.connector.JobDetails;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.output.direct.DirectOutputCommitter;
import com.google.cloud.hive.bigquery.connector.output.indirect.IndirectOutputCommitter;
import com.google.cloud.hive.bigquery.connector.utils.FileSystemUtils;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;

public class BigQueryOutputCommitter extends OutputCommitter {

  public static void commit(Configuration conf) throws IOException {
    JobDetails jobDetails = JobDetails.readJobDetailsFile(conf);
    String writeMethod =
        conf.get(HiveBigQueryConfig.WRITE_METHOD_KEY, HiveBigQueryConfig.WRITE_METHOD_DIRECT);
    // Pick the appropriate Committer class
    if (HiveBigQueryConfig.WRITE_METHOD_INDIRECT.equals(writeMethod)) {
      IndirectOutputCommitter.commitJob(conf, jobDetails);
    } else if (HiveBigQueryConfig.WRITE_METHOD_DIRECT.equals(writeMethod)) {
      DirectOutputCommitter.commitJob(conf, jobDetails);
    } else {
      throw new RuntimeException("Invalid write method setting: " + writeMethod);
    }
    FileSystemUtils.deleteWorkDirOnExit(conf);
  }

  /**
   * This method is called automatically at the end of a successful job when using the "mr"
   * execution engine. This method is not called when using "tez" -- for that, see {@link
   * BigQueryMetaHook#commitInsertTable(Table, boolean)}
   */
  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    JobConf conf = jobContext.getJobConf();
    String engine = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE);
    if (engine.equals("mr")) {
      commit(conf);
    } else {
      throw new RuntimeException("Unexpected execution engine: " + engine);
    }
    super.commitJob(jobContext);
  }

  /**
   * This method is called automatically at the end of a failed job when using the "mr" execution
   * engine.
   */
  @Override
  public void abortJob(JobContext jobContext, int status) throws IOException {
    JobConf conf = jobContext.getJobConf();
    JobDetails jobDetails = JobDetails.readJobDetailsFile(conf);
    DirectOutputCommitter.abortJob(conf, jobDetails);
    FileSystemUtils.deleteWorkDirOnExit(jobContext.getJobConf());
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
}
