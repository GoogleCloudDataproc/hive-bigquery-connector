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

import java.io.IOException;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;

/** Output committer compatible with the old "mapred" Hadoop API. */
public class BigQueryOutputCommitter extends OutputCommitter {

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    OutputCommitterUtils.commitJob(jobContext.getJobConf());
    super.commitJob(jobContext);
  }

  @Override
  public void abortJob(JobContext jobContext, int status) throws IOException {
    OutputCommitterUtils.abortJob(jobContext.getJobConf());
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
    // Tez needs HIVE-24629 or HIVE-27016 to do task commit
    // To-Do: add task commit, currently we are relying on task output file overwrites
  }

  @Override
  public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
    // Do nothing
  }
}
