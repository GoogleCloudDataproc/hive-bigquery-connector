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
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.hive.bigquery.connector.BigQuerySerDe;
import com.google.cloud.hive.bigquery.connector.JobDetails;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.output.WriterRegistry;
import com.google.cloud.hive.bigquery.connector.utils.JobUtils;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

/**
 * Writes records to a given BQ stream. Each task runs its own instance of this writer class, i.e.
 * each task creates a single BQ stream. The overall job committer is responsible for committing all
 * the streams to BigQuery later on at the end of the job.
 */
public class DirectRecordWriter
    implements org.apache.hadoop.mapred.RecordWriter<NullWritable, Writable>,
        FileSinkOperator.RecordWriter {

  JobConf jobConf;
  JobDetails jobDetails;
  BigQueryDirectDataWriterHelper streamWriter;
  StructObjectInspector rowObjectInspector;
  Descriptors.Descriptor descriptor;
  final Path streamRefFile;

  public DirectRecordWriter(JobConf jobConf, JobDetails jobDetails) {
    this.jobConf = jobConf;
    this.jobDetails = jobDetails;
    String taskAttemptID = UUID.randomUUID().toString();
    String writerId = WriterRegistry.getWriterId();
    streamRefFile =
        JobUtils.getTaskWriterOutputFile(
            jobDetails, taskAttemptID, writerId, HiveBigQueryConfig.STREAM_FILE_EXTENSION);
    this.rowObjectInspector = BigQuerySerDe.getRowObjectInspector(jobDetails.getTableProperties());
    try {
      descriptor = ProtoSchemaConverter.toDescriptor(this.rowObjectInspector);
    } catch (Descriptors.DescriptorValidationException e) {
      throw new RuntimeException(e);
    }
    ProtoSchema protoSchema =
        com.google.cloud.bigquery.storage.v1.ProtoSchemaConverter.convert(descriptor);
    this.streamWriter =
        DirectUtils.createStreamWriter(
            jobConf, jobDetails.getTableId(), jobDetails.getTableProperties(), protoSchema);
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
        ProtoDeserializer.buildSingleRowMessage(
            rowObjectInspector, descriptor, jobDetails.getBigquerySchema().getFields(), object);
    streamWriter.addRow(message.toByteString());
  }

  @Override
  public void close(boolean abort) throws IOException {
    if (abort) {
      streamWriter.abort();
    } else {
      // To-Do: find a better way to store streams info than small hdfs files
      // Create a stream reference file that contains the stream name, so we can retrieve
      // it later at the end of the job to commit all streams.
      streamWriter.finalizeStream();
      FSDataOutputStream outputStream = streamRefFile.getFileSystem(jobConf).create(streamRefFile);
      outputStream.write(streamWriter.getWriteStreamName().getBytes(StandardCharsets.UTF_8));
      outputStream.close();
    }
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    close(false);
  }
}
