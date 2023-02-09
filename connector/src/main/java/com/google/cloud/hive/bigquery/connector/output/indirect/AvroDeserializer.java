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

import com.google.cloud.hive.bigquery.connector.utils.DateTimeUtils;
import com.google.cloud.hive.bigquery.connector.utils.avro.AvroSchemaInfo;
import com.google.cloud.hive.bigquery.connector.utils.avro.AvroUtils;
import com.google.cloud.hive.bigquery.connector.utils.hive.KeyValueObjectInspector;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.serde2.io.*;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.*;

public class AvroDeserializer {

  /**
   * Converts the given Hive-serialized object into an Avro record, so it can later be written to
   * GCS and then loaded into BigQuery via the File Load API.
   */
  public static Record buildSingleRecord(
      StructObjectInspector soi, Schema avroSchema, Object object) {
    Record record = new Record(avroSchema);
    List<? extends StructField> allStructFieldRefs = soi.getAllStructFieldRefs();
    List<Object> structFieldsDataAsList = soi.getStructFieldsDataAsList(object);
    for (int fieldIndex = 0; fieldIndex < avroSchema.getFields().size(); fieldIndex++) {
      Object hiveValue = structFieldsDataAsList.get(fieldIndex);
      ObjectInspector fieldObjectInspector =
          allStructFieldRefs.get(fieldIndex).getFieldObjectInspector();
      String fieldName = allStructFieldRefs.get(fieldIndex).getFieldName();
      Schema fieldSchema = avroSchema.getField(fieldName).schema();
      Object avroValue = convertHiveValueToAvroValue(fieldObjectInspector, fieldSchema, hiveValue);
      record.put(fieldIndex, avroValue);
    }
    return record;
  }

  private static Object convertHiveValueToAvroValue(
      ObjectInspector fieldObjectInspector, Schema fieldSchema, Object fieldValue) {
    if (fieldValue == null) {
      return null;
    }

    AvroSchemaInfo schemaInfo = AvroUtils.getSchemaInfo(fieldSchema);

    if (fieldObjectInspector instanceof ListObjectInspector) { // Array type
      ListObjectInspector loi = (ListObjectInspector) fieldObjectInspector;
      ObjectInspector elementObjectInspector = loi.getListElementObjectInspector();
      Iterator<?> iterator = loi.getList(fieldValue).iterator();
      Schema elementSchema = schemaInfo.getActualSchema().getElementType();
      List<Object> array = new ArrayList<>();
      while (iterator.hasNext()) {
        Object elementValue = iterator.next();
        Object converted =
            convertHiveValueToAvroValue(elementObjectInspector, elementSchema, elementValue);
        array.add(converted);
      }
      return array;
    }

    if (fieldObjectInspector instanceof StructObjectInspector) { // Record/Struct type
      return buildSingleRecord(
          (StructObjectInspector) fieldObjectInspector, schemaInfo.getActualSchema(), fieldValue);
    }

    if (fieldObjectInspector instanceof MapObjectInspector) { // Map type
      // Convert the map into a list of key/value Avro records
      MapObjectInspector moi = (MapObjectInspector) fieldObjectInspector;
      List<Object> array = new ArrayList<>();
      Map<?, ?> map = moi.getMap(fieldValue);
      Schema valueSchema =
          schemaInfo
              .getActualSchema()
              .getElementType()
              .getField(KeyValueObjectInspector.VALUE_FIELD_NAME)
              .schema();
      Record record = new Record(schemaInfo.getActualSchema().getElementType());
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        Object key = entry.getKey().toString();
        Object convertedValue =
            convertHiveValueToAvroValue(
                moi.getMapValueObjectInspector(), valueSchema, entry.getValue());
        record.put(KeyValueObjectInspector.KEY_FIELD_NAME, key);
        record.put(KeyValueObjectInspector.VALUE_FIELD_NAME, convertedValue);
        array.add(record);
      }
      return array;
    }

    if (fieldObjectInspector instanceof ByteObjectInspector) { // Tiny Int
      return (int) ((ByteWritable) fieldValue).get();
    }

    if (fieldObjectInspector instanceof ShortObjectInspector) { // Small Int
      return (int) ((ShortWritable) fieldValue).get();
    }

    if (fieldObjectInspector instanceof IntObjectInspector) { // Regular Int
      return ((IntWritable) fieldValue).get();
    }

    if (fieldObjectInspector instanceof LongObjectInspector) { // Big Int
      return ((LongWritable) fieldValue).get();
    }

    if (fieldObjectInspector instanceof TimestampObjectInspector) {
      Timestamp timestamp = ((TimestampWritableV2) fieldValue).getTimestamp();
      return DateTimeUtils.getEpochMicrosFromHiveTimestamp(timestamp);
    }

    if (fieldObjectInspector instanceof TimestampLocalTZObjectInspector) {
      TimestampTZ timestampTZ = ((TimestampLocalTZWritable) fieldValue).getTimestampTZ();
      return DateTimeUtils.getEpochMicrosFromHiveTimestampTZ(timestampTZ);
    }

    if (fieldObjectInspector instanceof DateObjectInspector) {
      return ((DateWritableV2) fieldValue).getDays();
    }

    if (fieldObjectInspector instanceof FloatObjectInspector) {
      return ((FloatWritable) fieldValue).get();
    }

    if (fieldObjectInspector instanceof DoubleObjectInspector) {
      return ((DoubleWritable) fieldValue).get();
    }

    if (fieldObjectInspector instanceof BooleanObjectInspector) {
      return ((BooleanWritable) fieldValue).get();
    }

    if (fieldObjectInspector instanceof BinaryObjectInspector) {
      BytesWritable bytes = (BytesWritable) fieldValue;
      // Resize the bytes' array to remove any unnecessary extra capacity it might have
      bytes.setCapacity(bytes.getLength());
      // Wrap into a ByteBuffer
      ByteBuffer buffer = ByteBuffer.wrap(bytes.getBytes());
      return buffer.rewind();
    }

    if (fieldObjectInspector instanceof HiveCharObjectInspector) {
      return fieldValue.toString();
    }

    if (fieldObjectInspector instanceof HiveVarcharObjectInspector) {
      return fieldValue.toString();
    }

    if (fieldObjectInspector instanceof StringObjectInspector) {
      return fieldValue.toString();
    }

    if (fieldObjectInspector instanceof HiveDecimalObjectInspector) {
      if (fieldValue instanceof Buffer) {
        return fieldValue;
      }
      HiveDecimal decimal = ((HiveDecimalWritable) fieldValue).getHiveDecimal();
      int scale = ((HiveDecimalObjectInspector) fieldObjectInspector).scale();
      byte[] bytes = decimal.bigIntegerBytesScaled(scale);
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      return buffer.rewind();
    }

    String unsupportedCategory;
    if (fieldObjectInspector instanceof PrimitiveObjectInspector) {
      unsupportedCategory =
          ((PrimitiveObjectInspector) fieldObjectInspector).getPrimitiveCategory().name();
    } else {
      unsupportedCategory = fieldObjectInspector.getCategory().name();
    }

    throw new IllegalStateException("Unexpected type: " + unsupportedCategory);
  }
}
