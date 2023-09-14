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

import java.io.IOException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/** Output committer compatible with the new "mapreduce" Hadoop API. */
public class MapReduceOutputCommitter extends OutputCommitter {

  @Override
  public void commitJob(org.apache.hadoop.mapreduce.JobContext jobContext) throws IOException {
    BigQueryOutputCommitter.commitJob(jobContext.getConfiguration());
    super.commitJob(jobContext);
  }

  @Override
  public void abortJob(JobContext jobContext, JobStatus.State state) throws IOException {
    OutputCommitterUtils.abortJob(jobContext.getConfiguration());
    super.abortJob(jobContext, state);
  }

  @Override
  public void setupJob(org.apache.hadoop.mapreduce.JobContext jobContext) throws IOException {
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
