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

import com.google.cloud.hive.bigquery.connector.BigQuerySerDe;
import com.google.cloud.hive.bigquery.connector.input.BigQueryInputSplit;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.RecordReader;
import repackaged.by.hivebqconnector.org.apache.arrow.vector.FieldVector;
import repackaged.by.hivebqconnector.org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Reads from Arrow-formatted batches of rows, and returns individual rows in a serialized format
 * that Hive can handle.
 */
public class ArrowRecordReader
    implements org.apache.hadoop.mapred.RecordReader<NullWritable, ObjectWritable> {

  private VectorSchemaRoot currentBatch;
  private int numRowsLeftInBatch;
  private final RecordReader<NullWritable, VectorSchemaRoot> arrowBatchReader;
  private final List<String> columnNames;
  private final StructObjectInspector rowObjectInspector;

  public ArrowRecordReader(BigQueryInputSplit inputSplit, JobConf jobConf) {
    this.arrowBatchReader = new ArrowBatchReader(inputSplit, jobConf);
    this.columnNames = inputSplit.getColumnNames();
    this.rowObjectInspector = BigQuerySerDe.getRowObjectInspector(jobConf);
  }

  /**
   * Converts the given Arrow-formatted row into a serialized object made of "Writable" components
   * that Hive can handle.
   */
  private Object serializeRow(VectorSchemaRoot schemaRoot, int rowId) {
    int numColumnsInSchemaRoot = schemaRoot.getFieldVectors().size();
    Object[] row = new Object[columnNames.size()];
    for (int i = 0; i < numColumnsInSchemaRoot; i++) {
      FieldVector fieldVector = schemaRoot.getVector(i);
      int colIndex = columnNames.indexOf(fieldVector.getName());
      ObjectInspector fieldObjectInspector =
          rowObjectInspector.getStructFieldRef(fieldVector.getName()).getFieldObjectInspector();
      row[colIndex] = ArrowSerializer.serialize(fieldVector, fieldObjectInspector, rowId);
    }
    numRowsLeftInBatch--;
    return row;
  }

  @Override
  public float getProgress() throws IOException {
    // TODO
    return -1;
  }

  @Override
  public boolean next(NullWritable nullWritable, ObjectWritable objectWritable) throws IOException {
    try {
      if (numRowsLeftInBatch > 0) {
        // There still are some unprocessed rows in the current batch.
        // Serialize the next row in the batch.
        objectWritable.set(
            serializeRow(currentBatch, currentBatch.getRowCount() - numRowsLeftInBatch));
        return true;
      } else if (this.arrowBatchReader.nextKeyValue()) {
        // Get the next batch
        currentBatch = this.arrowBatchReader.getCurrentValue();
        numRowsLeftInBatch = currentBatch.getRowCount();
        // Serialize the first row in the batch
        objectWritable.set(serializeRow(currentBatch, 0));
        return true;
      }
      // No more rows to be processed
      return false;
    } catch (InterruptedException ex) {
      throw new IOException("Interrupted", ex);
    }
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
  public void close() throws IOException {
    arrowBatchReader.close();
  }
}
