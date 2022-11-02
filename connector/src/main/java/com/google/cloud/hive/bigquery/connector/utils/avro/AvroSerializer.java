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
package com.google.cloud.hive.bigquery.connector.utils.avro;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.io.*;

public class AvroSerializer {

  /**
   * Converts the given Avro-formatted value that was read from BigQuery to a serialized format that
   * Hive understands.
   */
  public static Object serialize(
      Object avroObject, ObjectInspector objectInspector, Schema schema) {
    AvroSchemaInfo schemaInfo = AvroUtils.getSchemaInfo(schema);

    if (avroObject == null) {
      if (!schemaInfo.isNullable()) {
        throw new IllegalArgumentException("Non-nullable field was null.");
      }
      return null;
    }

    Schema actualSchema = schemaInfo.getActualSchema();

    if (actualSchema.getType() == Schema.Type.ARRAY) {
      List<?> array = (List<?>) avroObject;
      ListObjectInspector loi = (ListObjectInspector) objectInspector;
      return array.stream()
          .map(
              value ->
                  serialize(
                      value, loi.getListElementObjectInspector(), actualSchema.getElementType()))
          .toArray();
    }

    if (actualSchema.getType() == Schema.Type.RECORD) {
      GenericRecord record = (GenericRecord) avroObject;
      List<Schema.Field> fields = actualSchema.getFields();
      StructObjectInspector soi = (StructObjectInspector) objectInspector;
      return fields.stream()
          .map(
              field ->
                  serialize(
                      record.get(field.name()),
                      soi.getStructFieldRef(field.name()).getFieldObjectInspector(),
                      field.schema()))
          .toArray();
    }

    if (actualSchema.getType() == Schema.Type.INT) {
      String logicalType = actualSchema.getProp("logicalType");
      if (logicalType != null && logicalType.equals("date")) {
        int intValue = (int) avroObject;
        LocalDate localDate = LocalDate.ofEpochDay(intValue);
        org.apache.hadoop.hive.common.type.Date date = new Date();
        date.setDayOfMonth(localDate.getDayOfMonth());
        date.setMonth(localDate.getMonth().getValue());
        date.setYear(localDate.getYear());
        return new DateWritableV2(date);
      }
      throw new UnsupportedOperationException(
          "Unsupported integer type: " + actualSchema.getType());
    }

    if (actualSchema.getType() == Schema.Type.LONG) {
      String logicalType = actualSchema.getProp("logicalType");
      if (logicalType != null && logicalType.equals("timestamp-micros")) {
        long longValue = (Long) avroObject;
        TimestampWritableV2 timestamp = new TimestampWritableV2();
        long secondsAsMillis = (longValue / 1_000_000) * 1_000;
        int nanos = (int) (longValue % 1_000_000) * 1_000;
        timestamp.setInternal(secondsAsMillis, nanos);
        return timestamp;
      }
      if (objectInspector instanceof ByteObjectInspector) { // Tiny Int
        return new ByteWritable(((Long) avroObject).byteValue());
      }
      if (objectInspector instanceof ShortObjectInspector) { // Small Int
        return new ShortWritable(((Long) avroObject).shortValue());
      }
      if (objectInspector instanceof IntObjectInspector) { // Regular Int
        return new IntWritable(((Long) avroObject).intValue());
      }
      // Big Int
      return new LongWritable((Long) avroObject);
    }

    if (actualSchema.getType() == Schema.Type.DOUBLE) {
      if (objectInspector instanceof FloatObjectInspector) {
        return new FloatWritable(((Double) avroObject).floatValue());
      }
      return new DoubleWritable((Double) avroObject);
    }

    if (actualSchema.getType() == Schema.Type.BOOLEAN) {
      return new BooleanWritable((Boolean) avroObject);
    }

    if (actualSchema.getType() == Schema.Type.BYTES) {
      byte[] bytes = ((ByteBuffer) avroObject).array();
      String logicalType = actualSchema.getProp("logicalType");
      if (logicalType != null && logicalType.equals("decimal")) {
        int scale = actualSchema.getJsonProp("scale").asInt();
        int precision = actualSchema.getJsonProp("precision").asInt();
        BigDecimal bigDecimal = new BigDecimal(new BigInteger(bytes), scale);
        HiveDecimal hiveDecimal = HiveDecimal.create(bigDecimal);
        HiveDecimal.enforcePrecisionScale(hiveDecimal, precision, scale);
        return new HiveDecimalWritable(hiveDecimal);
      } else {
        return new BytesWritable(bytes);
      }
    }

    if (actualSchema.getType() == Schema.Type.STRING) {
      return new Text(((Utf8) avroObject).toString());
    }

    throw new UnsupportedOperationException("Unsupported Avro type: " + schema);
  }
}
