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

import com.google.cloud.hive.bigquery.connector.utils.DateTimeUtils;
import com.google.cloud.hive.bigquery.connector.utils.avro.AvroSchemaInfo;
import com.google.cloud.hive.bigquery.connector.utils.avro.AvroUtils;
import com.google.cloud.hive.bigquery.connector.utils.hive.KeyValueObjectInspector;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.serde2.io.*;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
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

    if (objectInspector instanceof ListObjectInspector) { // Array/List type
      ListObjectInspector loi = (ListObjectInspector) objectInspector;
      List<?> array = (List<?>) avroObject;
      return array.stream()
          .map(
              value ->
                  serialize(
                      value, loi.getListElementObjectInspector(), actualSchema.getElementType()))
          .toArray();
    }

    if (objectInspector instanceof MapObjectInspector) { // Map type
      MapObjectInspector moi = (MapObjectInspector) objectInspector;
      List<?> array = (List<?>) avroObject;
      KeyValueObjectInspector kvoi = KeyValueObjectInspector.create(moi);
      Map<Object, Object> map = new HashMap<>();
      for (Object object : array) {
        Object[] item = (Object[]) serialize(object, kvoi, actualSchema.getElementType());
        map.put(item[0], item[1]);
      }
      return map;
    }

    if (objectInspector instanceof StructObjectInspector) { // Record/Struct type
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

    if (objectInspector instanceof DateObjectInspector) {
      return new DateWritableV2((int) avroObject);
    }

    if (objectInspector instanceof TimestampObjectInspector) {
      LocalDateTime localDateTime = LocalDateTime.parse(((Utf8) avroObject).toString());
      Timestamp timestamp = DateTimeUtils.getHiveTimestampFromLocalDatetime(localDateTime);
      TimestampWritableV2 timestampWritable = new TimestampWritableV2();
      timestampWritable.setInternal(timestamp.toEpochMilli(), timestamp.getNanos());
      return timestampWritable;
    }

    if (objectInspector instanceof TimestampLocalTZObjectInspector) {
      TimestampTZ timestampTZ = DateTimeUtils.getHiveTimestampTZFromUTC((long) avroObject);
      return new TimestampLocalTZWritable(timestampTZ);
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

    if (objectInspector instanceof LongObjectInspector) { // Big Int
      return new LongWritable((Long) avroObject);
    }

    if (objectInspector instanceof FloatObjectInspector) {
      return new FloatWritable(((Double) avroObject).floatValue());
    }

    if (objectInspector instanceof DoubleObjectInspector) {
      return new DoubleWritable((Double) avroObject);
    }

    if (objectInspector instanceof BooleanObjectInspector) {
      return new BooleanWritable((Boolean) avroObject);
    }

    if (objectInspector instanceof HiveDecimalObjectInspector) {
      byte[] bytes = ((ByteBuffer) avroObject).array();
      int scale = AvroUtils.getPropAsInt(actualSchema, "scale");
      BigDecimal bigDecimal = new BigDecimal(new BigInteger(bytes), scale);
      HiveDecimal hiveDecimal = HiveDecimal.create(bigDecimal);
      return new HiveDecimalWritable(hiveDecimal);
    }

    if (objectInspector instanceof BinaryObjectInspector) {
      byte[] bytes = ((ByteBuffer) avroObject).array();
      return new BytesWritable(bytes);
    }

    if (objectInspector instanceof StringObjectInspector
        || objectInspector instanceof HiveVarcharObjectInspector
        || objectInspector instanceof HiveCharObjectInspector) {
      return new Text(((Utf8) avroObject).toString());
    }

    throw new UnsupportedOperationException("Unsupported Avro type: " + schema);
  }
}
