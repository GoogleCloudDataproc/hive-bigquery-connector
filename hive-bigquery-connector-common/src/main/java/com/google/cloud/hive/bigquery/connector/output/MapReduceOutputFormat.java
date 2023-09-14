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
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** Output format compatible with the new "mapreduce" Hadoop API. This is used by Spark SQL. */
public class MapReduceOutputFormat<T> extends FileOutputFormat<Void, T> {

  private MapReduceOutputCommitter committer;

  @Override
  public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException {
    if (committer == null) {
      committer = new MapReduceOutputCommitter();
    }
    return committer;
  }

  @Override
  public RecordWriter<Void, T> getRecordWriter(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    // Note: Spark and Hive use `BigQueryOutputFormat.getRecordWriter()` instead.
    return null;
  }
}
