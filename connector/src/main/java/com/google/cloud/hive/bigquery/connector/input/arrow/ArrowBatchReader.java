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
package com.google.cloud.hive.bigquery.connector.input.arrow;

import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.hive.bigquery.connector.input.BigQueryInputSplit;
import java.io.*;
import java.util.Collections;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import shaded.hivebqcon.com.google.cloud.bigquery.connector.common.ArrowReaderIterator;
import shaded.hivebqcon.com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import shaded.hivebqcon.com.google.protobuf.ByteString;
import shaded.hivebqcon.org.apache.arrow.memory.BufferAllocator;
import shaded.hivebqcon.org.apache.arrow.memory.RootAllocator;
import shaded.hivebqcon.org.apache.arrow.vector.VectorSchemaRoot;
import shaded.hivebqcon.org.apache.arrow.vector.ipc.ArrowStreamReader;

/**
 * Reads Arrow-formatted batches of rows (in the form of VectorSchemaRoot objects) from the BigQuery
 * Storage Read API responses.
 */
public class ArrowBatchReader extends RecordReader<NullWritable, VectorSchemaRoot> {

  private VectorSchemaRoot current;
  private final Iterator<ReadRowsResponse> responseIterator;
  private Iterator<VectorSchemaRoot> arrowReaderIterator;
  private final BufferAllocator bufferAllocator;
  private ByteString schema;

  public ArrowBatchReader(BigQueryInputSplit inputSplit, Configuration conf) {
    ReadRowsHelper readRowsHelper = inputSplit.getReadRowsHelper();
    responseIterator = readRowsHelper.readRows();
    arrowReaderIterator = Collections.emptyIterator();
    bufferAllocator = new RootAllocator(Long.MAX_VALUE);
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext context) {}

  /**
   * Fetches a new VectorSchemaRoot, i.e. a batch of Arrow-formatted data, from the BQ read
   * response.
   */
  @Override
  public boolean nextKeyValue() {
    if (responseIterator.hasNext() && !arrowReaderIterator.hasNext()) {
      ReadRowsResponse response = responseIterator.next();
      if (schema == null) {
        // Retrieve the Arrow schema from the BQ read response
        schema = response.getArrowSchema().getSerializedSchema();
      }
      ByteArrayInputStream schemaInputStream = new ByteArrayInputStream(schema.toByteArray());
      // Retrieve the Arrow data
      ByteString batchData = response.getArrowRecordBatch().getSerializedRecordBatch();
      ByteArrayInputStream batchInputStream = new ByteArrayInputStream(batchData.toByteArray());
      // Merge the schema and data together into a single input stream and create
      // a new Arrow reader
      SequenceInputStream bytesWithSchemaStream =
          new SequenceInputStream(schemaInputStream, batchInputStream);
      ArrowStreamReader reader = new ArrowStreamReader(bytesWithSchemaStream, bufferAllocator);
      arrowReaderIterator = new ArrowReaderIterator(reader);
    }
    if (arrowReaderIterator.hasNext()) {
      current = arrowReaderIterator.next();
      return true;
    }
    current = null;
    return false;
  }

  @Override
  public NullWritable getCurrentKey() {
    return NullWritable.get();
  }

  @Override
  public VectorSchemaRoot getCurrentValue() {
    return current;
  }

  @Override
  public float getProgress() {
    // TODO
    return -1;
  }

  @Override
  public void close() {}
}
