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
import com.google.cloud.hive.bigquery.connector.output.direct.DirectRecordWriter;
import com.google.cloud.hive.bigquery.connector.output.indirect.IndirectAvroRecordWriter;
import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.Progressable;

public class BigQueryOutputFormat
    implements OutputFormat<NullWritable, Writable>, HiveOutputFormat<NullWritable, Writable> {

  /**
   * Get the RecordWriter (direct or indirect) for the given job. Params: fileSystem - Ignored job –
   * configuration for the job whose output is being written. name – the unique name for this part
   * of the output. progress – mechanism for reporting progress while writing to file. Returns: a
   * RecordWriter to write the output for the job. Throws: IOException
   */
  @Override
  public org.apache.hadoop.mapred.RecordWriter<NullWritable, Writable> getRecordWriter(
      FileSystem fileSystem, JobConf jobConf, String name, Progressable progressable)
      throws IOException {
    throw new RuntimeException("Code path not expected");
  }

  @Override
  public RecordWriter getHiveRecordWriter(
      JobConf jobConf,
      Path path,
      Class<? extends Writable> aClass,
      boolean b,
      Properties tableProperties,
      Progressable progressable)
      throws IOException {
    JobDetails jobDetails = JobDetails.getJobDetails(jobConf);
    OutputPartition partition = OutputPartition.getFromJobDetails(jobDetails);

    // Pick the appropriate RecordWriter (direct or indirect) based on the configured write method
    String writeMethod =
        jobConf.get(HiveBigQueryConfig.WRITE_METHOD_KEY, HiveBigQueryConfig.WRITE_METHOD_DIRECT);
    if (HiveBigQueryConfig.WRITE_METHOD_INDIRECT.equals(writeMethod)) {
      return new IndirectAvroRecordWriter(jobConf, jobDetails, partition);
    } else if (HiveBigQueryConfig.WRITE_METHOD_DIRECT.equals(writeMethod)) {
      return new DirectRecordWriter(jobConf, jobDetails, partition);
    } else {
      throw new RuntimeException("Invalid write mode: " + writeMethod);
    }
  }

  @Override
  public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {
    // Do nothing
  }
}
