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

import com.google.cloud.hive.bigquery.connector.BigQuerySerDe;
import com.google.cloud.hive.bigquery.connector.JobDetails;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.output.WriterRegistry;
import com.google.cloud.hive.bigquery.connector.utils.JobUtils;
import com.google.cloud.hive.bigquery.connector.utils.avro.AvroUtils;
import com.google.cloud.hive.bigquery.connector.utils.hive.HiveUtils;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptID;

/**
 * Writes records to an Avro file in GCS. Each task runs its own instance of this writer class, i.e.
 * each task creates a single Avro file. The overall job committer is responsible for loading all
 * the Avro files to BigQuery later on at the end of the job.
 */
public class IndirectAvroRecordWriter
    implements org.apache.hadoop.mapred.RecordWriter<NullWritable, Writable>,
        FileSinkOperator.RecordWriter {

  JobConf jobConf;
  JobDetails jobDetails;
  StructObjectInspector rowObjectInspector;
  Schema avroSchema;
  final DataFileWriter<GenericRecord> dataFileWriter;
  final String writerId;

  public IndirectAvroRecordWriter(JobConf jobConf, JobDetails jobDetails) {
    this.jobConf = jobConf;
    this.jobDetails = jobDetails;
    this.rowObjectInspector = BigQuerySerDe.getRowObjectInspector(jobDetails.getTableProperties());
    this.avroSchema = jobDetails.getAvroSchema();
    this.writerId = WriterRegistry.getWriterId();
    TaskAttemptID taskAttemptID = HiveUtils.taskAttemptIDWrapper(jobConf);
    Path filePath =
        JobUtils.getTaskWriterOutputFile(
            jobDetails, taskAttemptID, writerId, HiveBigQueryConfig.LOAD_FILE_EXTENSION);
    this.dataFileWriter = AvroUtils.createDataFileWriter(jobConf, jobDetails, filePath);
  }

  @Override
  public void write(NullWritable nullWritable, Writable writable) throws IOException {
    write(writable);
  }

  @Override
  public void write(Writable writable) throws IOException {
    Object serializedRecord = ((ObjectWritable) writable).get();
    GenericRecord record =
        AvroDeserializer.buildSingleRecord(rowObjectInspector, avroSchema, serializedRecord);
    dataFileWriter.append(record);
  }

  @Override
  public void close(boolean abort) throws IOException {
    dataFileWriter.close();
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    close(false);
  }
}
