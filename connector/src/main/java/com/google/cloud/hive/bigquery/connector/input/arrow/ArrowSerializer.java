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

import com.google.cloud.hive.bigquery.connector.utils.DateTimeUtils;
import com.google.cloud.hive.bigquery.connector.utils.hive.KeyValueObjectInspector;
import java.math.BigDecimal;
import java.time.*;
import java.util.*;
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
import shaded.hivebqcon.org.apache.arrow.vector.*;
import shaded.hivebqcon.org.apache.arrow.vector.complex.ListVector;
import shaded.hivebqcon.org.apache.arrow.vector.complex.StructVector;

public class ArrowSerializer {

  /**
   * Converts the given Arrow-formatted value that was read from BigQuery to a serialized format
   * that Hive understands.
   */
  public static Object serialize(Object value, ObjectInspector objectInspector, int rowId) {
    if (value == null || (value instanceof ValueVector && ((ValueVector) value).isNull(rowId))) {
      return null;
    }

    if (objectInspector instanceof BooleanObjectInspector) {
      return new BooleanWritable(((BitVector) value).get(rowId) == 1);
    }

    if (objectInspector instanceof ByteObjectInspector) { // Tiny Int
      return new ByteWritable((byte) ((BigIntVector) value).get(rowId));
    }

    if (objectInspector instanceof ShortObjectInspector) { // Small Int
      return new ShortWritable((short) ((BigIntVector) value).get(rowId));
    }

    if (objectInspector instanceof IntObjectInspector) { // Regular Int
      return new IntWritable((int) ((BigIntVector) value).get(rowId));
    }

    if (objectInspector instanceof LongObjectInspector) { // Big Int
      return new LongWritable(((BigIntVector) value).get(rowId));
    }

    if (objectInspector instanceof FloatObjectInspector) {
      return new FloatWritable((float) ((Float8Vector) value).get(rowId));
    }

    if (objectInspector instanceof DoubleObjectInspector) {
      return new DoubleWritable(((Float8Vector) value).get(rowId));
    }

    if (objectInspector instanceof HiveDecimalObjectInspector) {
      BigDecimal decimalValue = ((DecimalVector) value).getObject(rowId);
      HiveDecimal hiveDecimal = HiveDecimal.create(decimalValue);
      return new HiveDecimalWritable(hiveDecimal);
    }

    if (objectInspector instanceof StringObjectInspector
        || objectInspector instanceof HiveVarcharObjectInspector
        || objectInspector instanceof HiveCharObjectInspector) {
      VarCharVector v = (VarCharVector) value;
      if (v.isSet(rowId) == 0) {
        return null;
      } else {
        return new Text(v.getObject(rowId).toString());
      }
    }

    if (objectInspector instanceof BinaryObjectInspector) {
      return new BytesWritable(((VarBinaryVector) value).getObject(rowId));
    }

    if (objectInspector instanceof DateObjectInspector) {
      return new DateWritableV2(((DateDayVector) value).get(rowId));
    }

    if (objectInspector instanceof TimestampObjectInspector) {
      LocalDateTime localDateTime = ((TimeStampMicroVector) value).getObject(rowId);
      Timestamp timestamp = DateTimeUtils.getHiveTimestampFromLocalDatetime(localDateTime);
      return new TimestampWritableV2(timestamp);
    }

    if (objectInspector instanceof TimestampLocalTZObjectInspector) {
      long longValue = ((TimeStampMicroTZVector) value).get(rowId);
      TimestampTZ timestampTZ = DateTimeUtils.getHiveTimestampTZFromUTC(longValue);
      return new TimestampLocalTZWritable(timestampTZ);
    }

    if (objectInspector instanceof ListObjectInspector) { // Array/List type
      ListObjectInspector loi = (ListObjectInspector) objectInspector;
      ListVector listVector = (ListVector) value;
      int start = listVector.getOffsetBuffer().getInt(rowId * 4L);
      int end = listVector.getOffsetBuffer().getInt((rowId + 1) * 4L);
      int numItems = end - start;
      Object[] children = new Object[numItems];
      for (int i = 0; i < numItems; i++) {
        children[i] =
            serialize(listVector.getDataVector(), loi.getListElementObjectInspector(), start + i);
      }
      return children;
    }

    if (objectInspector instanceof MapObjectInspector) { // Map type
      MapObjectInspector moi = (MapObjectInspector) objectInspector;
      Map<Object, Object> map = new HashMap<>();
      ListVector listVector = (ListVector) value;
      StructVector structVector = (StructVector) listVector.getDataVector();
      int start = listVector.getOffsetBuffer().getInt(rowId * 4L);
      int end = listVector.getOffsetBuffer().getInt((rowId + 1) * 4L);
      ValueVector keys =
          structVector.getChildVectorWithOrdinal(KeyValueObjectInspector.KEY_FIELD_NAME).vector;
      ValueVector values =
          structVector.getChildVectorWithOrdinal(KeyValueObjectInspector.VALUE_FIELD_NAME).vector;
      int numItems = end - start;
      for (int i = 0; i < numItems; i++) {
        Object k = serialize(keys, moi.getMapKeyObjectInspector(), start + i);
        Object v = serialize(values, moi.getMapValueObjectInspector(), start + i);
        map.put(k, v);
      }
      return map;
    }

    if (objectInspector instanceof StructObjectInspector) { // Record/Struct type
      StructObjectInspector soi = (StructObjectInspector) objectInspector;
      List<FieldVector> fieldVectors = ((StructVector) value).getChildrenFromFields();
      Object[] row = new Object[fieldVectors.size()];
      int i = 0;
      for (FieldVector fieldVector : fieldVectors) {
        row[i] =
            serialize(
                fieldVector, soi.getAllStructFieldRefs().get(i).getFieldObjectInspector(), rowId);
        i++;
      }
      return row;
    }
    throw new UnsupportedOperationException(
        "Unsupported ObjectInspector `"
            + objectInspector.getClass().getName()
            + "` for value of type `"
            + value.getClass().getName()
            + "`");
  }
}
