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
package com.google.cloud.hive.bigquery.connector.output.direct;

import com.google.cloud.bigquery.connector.common.BigQueryDirectDataWriterHelper;
import com.google.cloud.hive.bigquery.connector.BigQuerySerDe;
import com.google.cloud.hive.bigquery.connector.JobInfo;
import com.google.cloud.hive.bigquery.connector.utils.HiveUtils;
import com.google.cloud.hive.bigquery.connector.utils.proto.ProtoDeserializer;
import com.google.cloud.hive.bigquery.connector.utils.proto.ProtoSchemaConverter;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import repackaged.by.hivebqconnector.com.google.protobuf.Descriptors;
import repackaged.by.hivebqconnector.com.google.protobuf.DynamicMessage;

/**
 * Writes records to a given BQ stream. Each task runs its own instance of this writer class, i.e.
 * each task creates a single BQ stream. The overall job committer is responsible for committing all
 * the streams to BigQuery later on at the end of the job.
 */
public class DirectRecordWriter
    implements org.apache.hadoop.mapred.RecordWriter<NullWritable, Writable>,
        FileSinkOperator.RecordWriter {

  JobConf jobConf;
  TaskAttemptID taskAttemptID;
  BigQueryDirectDataWriterHelper streamWriter;
  StructObjectInspector rowObjectInspector;
  Descriptors.Descriptor descriptor;

  public DirectRecordWriter(JobConf jobConf, JobInfo jobInfo) {
    this.jobConf = jobConf;
    this.taskAttemptID = HiveUtils.taskAttemptIDWrapper(jobConf);
    this.streamWriter =
        DirectUtils.createStreamWriter(jobConf, jobInfo.getTableId(), jobInfo.getProtoSchema());
    this.rowObjectInspector = BigQuerySerDe.getRowObjectInspector(jobInfo.getTableProperties());
    try {
      descriptor = ProtoSchemaConverter.toDescriptor(this.rowObjectInspector);
    } catch (Descriptors.DescriptorValidationException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void write(NullWritable nullWritable, Writable writable) throws IOException {
    write(writable);
  }

  /** Appends the row to the BQ stream. */
  @Override
  public void write(Writable writable) throws IOException {
    Object object = ((ObjectWritable) writable).get();
    DynamicMessage message =
        ProtoDeserializer.buildSingleRowMessage(rowObjectInspector, descriptor, object);
    streamWriter.addRow(message.toByteString());
  }

  @Override
  public void close(boolean abort) throws IOException {
    // Only save the stream reference file if the task has succeeded
    if (!abort) {
      // Create a stream reference file that contains the stream name, so we can retrieve
      // it later at the end of the job to commit all streams.
      streamWriter.commit(); // TODO: Ideally that method should be renamed to "finalize()"
      JobInfo jobInfo = JobInfo.readInfoFile(jobConf);
      Path filePath =
          DirectUtils.getTaskTempStreamFile(jobConf, jobInfo.getTableId(), taskAttemptID);
      FSDataOutputStream streamFile = filePath.getFileSystem(jobConf).create(filePath);
      streamFile.write(streamWriter.getWriteStreamName().getBytes());
      streamFile.close();
    }
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    close(false);
  }
}
