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

import com.google.cloud.hive.bigquery.connector.JobInfo;
import com.google.cloud.hive.bigquery.connector.config.RunConf;
import com.google.cloud.hive.bigquery.connector.config.RunConf.Config;
import com.google.cloud.hive.bigquery.connector.output.direct.DirectRecordWriter;
import com.google.cloud.hive.bigquery.connector.output.fileload.FileLoadAvroRecordWriter;
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

  @Override
  public org.apache.hadoop.mapred.RecordWriter<NullWritable, Writable> getRecordWriter(
      FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable)
      throws IOException {
    // Pick the appropriate RecordWriter class based on configuration
    JobInfo jobInfo = JobInfo.readInfoFile(jobConf);
    String writeMethod = Config.WRITE_METHOD.get(jobConf);
    if (RunConf.WRITE_METHOD_FILE_LOAD.equals(writeMethod)) {
      return new FileLoadAvroRecordWriter(jobConf, jobInfo);
    } else if (RunConf.WRITE_METHOD_DIRECT.equals(writeMethod)) {
      return new DirectRecordWriter(jobConf, jobInfo);
    } else {
      throw new RuntimeException("Invalid write mode: " + writeMethod);
    }
  }

  @Override
  public RecordWriter getHiveRecordWriter(
      JobConf jobConf,
      Path path,
      Class<? extends Writable> aClass,
      boolean b,
      Properties properties,
      Progressable progressable)
      throws IOException {
    return (RecordWriter) getRecordWriter(null, jobConf, null, null);
  }

  @Override
  public void checkOutputSpecs(FileSystem fileSystem, JobConf jobConf) throws IOException {
    // Do nothing
  }
}
