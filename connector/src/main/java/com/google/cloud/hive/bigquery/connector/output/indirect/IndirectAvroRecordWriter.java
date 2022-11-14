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
import com.google.cloud.hive.bigquery.connector.utils.HiveUtils;
import com.google.cloud.hive.bigquery.connector.utils.avro.AvroDeserializer;
import com.google.cloud.hive.bigquery.connector.utils.avro.AvroSchemaInfo;
import com.google.cloud.hive.bigquery.connector.utils.avro.AvroUtils;
import com.google.cloud.hive.bigquery.connector.utils.avro.AvroUtils.AvroOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
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
    this.avroOutput = AvroOutput.initialize(jobConf, jobDetails.getAvroSchema());
    this.avroSchema = fixAvroSchema(jobDetails.getAvroSchema());
    this.rowObjectInspector = BigQuerySerDe.getRowObjectInspector(jobDetails.getTableProperties());
  }

  private Schema fixAvroSchema(Schema originalSchema) {
    AvroSchemaInfo schemaInfo = AvroUtils.getSchemaInfo(originalSchema);
    Schema schema = schemaInfo.getActualSchema();

    if (schema.getType() == Schema.Type.ARRAY) {
      Schema fixedElementType = fixAvroSchema(schema.getElementType());
      schema = Schema.createArray(fixedElementType);
    }

    else if (schema.getType() == Schema.Type.RECORD) {
      List<Schema.Field> originalFields = schema.getFields();
      List<Schema.Field> fixedFields = new ArrayList<>();
      originalFields.forEach(originalField -> {
        Schema fixedFieldSchema = fixAvroSchema(originalField.schema());
        fixedFields.add(new Schema.Field(originalField.name(), fixedFieldSchema, originalField.doc(), originalField.defaultValue()));
      });
      schema = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), false);
      schema.setFields(fixedFields);
    }

    else if (schema.getType() == Schema.Type.BYTES) {
      String logicalType = schema.getProp("logicalType");
      if (logicalType != null && logicalType.equals("decimal")) {
        schema = Schema.create(Schema.Type.BYTES);
        schema.addProp("logicalType", "decimal");
        schema.addProp("precision", "77");
        schema.addProp("scale", "38");
      }
    }

    if (schemaInfo.isNullable()) {
      schema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), schema));
    }

    return schema;
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
