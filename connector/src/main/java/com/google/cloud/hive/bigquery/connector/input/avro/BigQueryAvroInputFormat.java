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
package com.google.cloud.hive.bigquery.connector.input.avro;

import com.google.cloud.hive.bigquery.connector.input.BigQueryInputSplit;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class BigQueryAvroInputFormat
    implements org.apache.hadoop.mapred.InputFormat<NullWritable, ObjectWritable> {

  /**
   * Creates hadoop splits (i.e BigQuery streams) so that each task can read data from the
   * corresponding stream.
   *
   * @param jobConf The job's configuration
   * @param numSplits Number of splits requested by MapReduce, but ignored as BigQuery decides the
   *     number of streams used in the read session.
   * @return InputSplit[] - Collection of FileSplits
   */
  @Override
  public InputSplit[] getSplits(JobConf jobConf, int numSplits) {
    return BigQueryInputSplit.createSplitsFromBigQueryReadStreams(jobConf);
  }

  @Override
  public RecordReader<NullWritable, ObjectWritable> getRecordReader(
      InputSplit inputSplit, JobConf jobConf, Reporter reporter) {
    return new AvroRecordReader((BigQueryInputSplit) inputSplit);
  }
}
