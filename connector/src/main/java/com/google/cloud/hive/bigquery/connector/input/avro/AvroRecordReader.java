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

import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.hive.bigquery.connector.BigQuerySerDe;
import com.google.cloud.hive.bigquery.connector.input.BigQueryInputSplit;
import com.google.cloud.hive.bigquery.connector.utils.avro.AvroUtils;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import shaded.hivebqcon.com.google.protobuf.ByteString;

/**
 * Reads Avro-formatted records returned by the BigQuery Storage Read API responses and serializes
 * those records into a format that Hive can handle.
 */
public class AvroRecordReader implements RecordReader<NullWritable, ObjectWritable> {

  private final Parser parser = new Parser();
  private final Iterator<ReadRowsResponse> responseIterator;
  private Iterator<GenericRecord> recordIterator;
  private Schema schema;
  private final List<String> columnNames;
  private final StructObjectInspector rowObjectInspector;
  private final JobConf jobConf;

  public AvroRecordReader(BigQueryInputSplit inputSplit, JobConf jobConf) {
    this.jobConf = jobConf;
    this.responseIterator = inputSplit.getReadRowsHelper().readRows();
    this.recordIterator = Collections.emptyIterator();
    this.columnNames = inputSplit.getColumnNames();
    this.rowObjectInspector = BigQuerySerDe.getRowObjectInspector(jobConf);
  }

  /**
   * Converts the given Avro-formatted record into a serialized object made of "Writable" components
   * that Hive can handle.
   */
  private Object serializeRow(GenericRecord record) {
    Schema actualSchema = AvroUtils.getSchemaInfo(record.getSchema()).getActualSchema();
    List<Schema.Field> fields = actualSchema.getFields();
    Object[] row = new Object[columnNames.size()];
    for (Schema.Field field : fields) {
      int colIndex = columnNames.indexOf(field.name());
      ObjectInspector fieldObjectInspector =
          rowObjectInspector.getStructFieldRef(field.name()).getFieldObjectInspector();
      row[colIndex] =
          AvroSerializer.serialize(record.get(field.name()), fieldObjectInspector, field.schema());
    }
    return row;
  }

  @Override
  public float getProgress() {
    // TODO
    return -1;
  }

  @Override
  public boolean next(NullWritable nullWritable, ObjectWritable objectWritable) {
    if (responseIterator.hasNext() && !recordIterator.hasNext()) {
      ReadRowsResponse response = responseIterator.next();
      if (schema == null) {
        schema = parser.parse(response.getAvroSchema().getSchema());
      }
      recordIterator =
          new AvroRecordIterator(schema, response.getAvroRows().getSerializedBinaryRows());
    }
    if (recordIterator.hasNext()) {
      GenericRecord avroRecord = recordIterator.next();
      objectWritable.set(serializeRow(avroRecord));
      return true;
    }
    return false;
  }

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public ObjectWritable createValue() {
    return new ObjectWritable();
  }

  @Override
  public long getPos() throws IOException {
    // TODO
    return -1;
  }

  @Override
  public void close() {}

  private static class AvroRecordIterator implements Iterator<GenericRecord> {

    private final BinaryDecoder in;
    private final GenericDatumReader<GenericRecord> reader;

    AvroRecordIterator(Schema schema, ByteString bytes) {
      reader = new GenericDatumReader<>(schema);
      in = new DecoderFactory().binaryDecoder(bytes.toByteArray(), null);
    }

    @Override
    public boolean hasNext() {
      try {
        return !in.isEnd();
      } catch (IOException e) {
        throw new RuntimeException("Failed to check for more records", e);
      }
    }

    @Override
    public GenericRecord next() {
      try {
        return reader.read(/* reuse= */ null, in);
      } catch (IOException e) {
        throw new RuntimeException("Failed to read more records", e);
      }
    }
  }
}
