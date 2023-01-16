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
import com.google.cloud.hive.bigquery.connector.utils.avro.AvroUtils;
import com.google.cloud.hive.bigquery.connector.utils.avro.AvroUtils.AvroOutput;
import com.google.cloud.hive.bigquery.connector.utils.hive.HiveUtils;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
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

  AvroOutput avroOutput;
  JobConf jobConf;
  TaskAttemptID taskAttemptID;
  StructObjectInspector rowObjectInspector;
  Schema avroSchema;

  public IndirectAvroRecordWriter(JobConf jobConf, JobDetails jobDetails) {
    this.jobConf = jobConf;
    this.taskAttemptID = HiveUtils.taskAttemptIDWrapper(jobConf);
    this.avroSchema =
        AvroUtils.adaptSchemaForBigQuery(
            AvroUtils.extractAvroSchema(jobConf, jobDetails.getTableProperties()));
    this.avroOutput = AvroOutput.initialize(jobConf, this.avroSchema);
    this.rowObjectInspector = BigQuerySerDe.getRowObjectInspector(jobDetails.getTableProperties());
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
    this.avroOutput.getDataFileWriter().append(record);
  }

  @Override
  public void close(boolean abort) throws IOException {
    if (!abort) {
      JobDetails jobDetails = JobDetails.readJobDetailsFile(jobConf);
      Path filePath =
          IndirectUtils.getTaskAvroTempFile(
              jobConf, jobDetails.getTableId(), jobDetails.getGcsTempPath(), taskAttemptID);
      FileSystem fileSystem = filePath.getFileSystem(jobConf);
      FSDataOutputStream fsDataOutputStream = fileSystem.create(filePath);
      avroOutput.getDataFileWriter().flush();
      fsDataOutputStream.write(avroOutput.getOutputStream().toByteArray());
      fsDataOutputStream.close();
    }
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    close(false);
  }
}
