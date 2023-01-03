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

import com.google.cloud.hive.bigquery.connector.utils.hive.KeyValueObjectInspector;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.*;
import repackaged.by.hivebqconnector.org.apache.arrow.vector.*;
import repackaged.by.hivebqconnector.org.apache.arrow.vector.complex.ListVector;
import repackaged.by.hivebqconnector.org.apache.arrow.vector.complex.StructVector;
import repackaged.by.hivebqconnector.org.apache.arrow.vector.util.JsonStringArrayList;
import repackaged.by.hivebqconnector.org.apache.arrow.vector.util.JsonStringHashMap;

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
      boolean booleanValue;
      if (value instanceof BitVector) {
        booleanValue = ((BitVector) value).get(rowId) == 1;
      } else {
        booleanValue = (boolean) value;
      }
      return new BooleanWritable(booleanValue);
    }

    if (objectInspector instanceof ByteObjectInspector) { // Tiny Int
      byte byteValue;
      if (value instanceof BigIntVector) {
        byteValue = (byte) ((BigIntVector) value).get(rowId);
      } else {
        byteValue = ((Long) value).byteValue();
      }
      return new ByteWritable(byteValue);
    }

    if (objectInspector instanceof ShortObjectInspector) { // Small Int
      short shortValue;
      if (value instanceof BigIntVector) {
        shortValue = (short) ((BigIntVector) value).get(rowId);
      } else {
        shortValue = ((Long) value).shortValue();
      }
      return new ShortWritable(shortValue);
    }

    if (objectInspector instanceof IntObjectInspector) { // Regular Int
      int intValue;
      if (value instanceof BigIntVector) {
        intValue = (int) ((BigIntVector) value).get(rowId);
      } else {
        intValue = ((Long) value).intValue();
      }
      return new IntWritable(intValue);
    }

    if (objectInspector instanceof LongObjectInspector) { // Big Int
      long longValue;
      if (value instanceof BigIntVector) {
        longValue = ((BigIntVector) value).get(rowId);
      } else {
        longValue = (long) value;
      }
      return new LongWritable(longValue);
    }

    if (objectInspector instanceof FloatObjectInspector) {
      float floatValue;
      if (value instanceof Float8Vector) {
        floatValue = (float) ((Float8Vector) value).get(rowId);
      } else {
        floatValue = ((Double) value).floatValue();
      }
      return new FloatWritable(floatValue);
    }

    if (objectInspector instanceof DoubleObjectInspector) {
      double doubleValue;
      if (value instanceof Float8Vector) {
        doubleValue = ((Float8Vector) value).get(rowId);
      } else {
        doubleValue = (double) value;
      }
      return new DoubleWritable(doubleValue);
    }

    if (objectInspector instanceof HiveDecimalObjectInspector) {
      BigDecimal decimalValue;
      if (value instanceof DecimalVector) {
        decimalValue = ((DecimalVector) value).getObject(rowId);
      } else {
        decimalValue = (BigDecimal) value;
      }
      HiveDecimal hiveDecimal = HiveDecimal.create(decimalValue);
      HiveDecimal.enforcePrecisionScale(
          hiveDecimal, decimalValue.precision(), decimalValue.scale());
      return new HiveDecimalWritable(hiveDecimal);
    }

    if (objectInspector instanceof StringObjectInspector
        || objectInspector instanceof HiveVarcharObjectInspector
        || objectInspector instanceof HiveCharObjectInspector) {
      if (value instanceof VarCharVector) {
        VarCharVector v = (VarCharVector) value;
        if (v.isSet(rowId) == 0) {
          return null;
        } else {
          return new Text(v.getObject(rowId).toString());
        }
      } else {
        return new Text(value.toString());
      }
    }

    if (objectInspector instanceof BinaryObjectInspector) {
      byte[] byteArray;
      if (value instanceof VarBinaryVector) {
        byteArray = ((VarBinaryVector) value).getObject(rowId);
      } else {
        byteArray = (byte[]) value;
      }
      return new BytesWritable(byteArray);
    }

    if (objectInspector instanceof DateObjectInspector) {
      int intValue;
      if (value instanceof DateDayVector) {
        intValue = ((DateDayVector) value).get(rowId);
      } else {
        intValue = (int) value;
      }
      LocalDate localDate = LocalDate.ofEpochDay(intValue);
      Date date = new Date();
      date.setDayOfMonth(localDate.getDayOfMonth());
      date.setMonth(localDate.getMonth().getValue());
      date.setYear(localDate.getYear());
      return new DateWritableV2(date);
    }

    if (objectInspector instanceof TimestampObjectInspector) {
      if (value instanceof TimeStampMicroTZVector) {
        long longValue = ((TimeStampMicroTZVector) value).get(rowId);
        TimestampWritableV2 timestamp = new TimestampWritableV2();
        long secondsAsMillis = (longValue / 1_000_000) * 1_000;
        int nanos = (int) (longValue % 1_000_000) * 1_000;
        timestamp.setInternal(secondsAsMillis, nanos);
        return timestamp;
      }
      if (value instanceof TimeStampMicroVector) {
        LocalDateTime localDateTime = ((TimeStampMicroVector) value).getObject(rowId);
        TimestampWritableV2 timestamp = new TimestampWritableV2();
        timestamp.setInternal(localDateTime.toEpochSecond(ZoneOffset.UTC), localDateTime.getNano());
        return timestamp;
      }
      throw new RuntimeException("Unexpected timestamp type:" + value.getClass().getName());
    }

    if (objectInspector instanceof ListObjectInspector) { // Array/List type
      ListObjectInspector loi = (ListObjectInspector) objectInspector;
      JsonStringArrayList<?> listData;
      if (value instanceof ListVector) {
        ListVector listVector = (ListVector) value;
        listData = (JsonStringArrayList<?>) listVector.getObject(rowId);
      } else if (value instanceof JsonStringArrayList) {
        listData = (JsonStringArrayList) value;
      } else {
        throw new RuntimeException("Unexpected list type:" + value.getClass().getName());
      }
      int numItems = listData.size();
      Object[] children = new Object[numItems];
      for (int i = 0; i < numItems; i++) {
        children[i] = serialize(listData.get(i), loi.getListElementObjectInspector(), i);
      }
      return children;
    }

    if (objectInspector instanceof MapObjectInspector) { // Map type
      MapObjectInspector moi = (MapObjectInspector) objectInspector;
      Map<Object, Object> map = new HashMap<>();
      JsonStringArrayList<Map<?, ?>> list;
      if (value instanceof JsonStringArrayList) {
        list = (JsonStringArrayList<Map<?, ?>>) value;
      } else if (value instanceof ListVector) {
        ListVector listVector = (ListVector) value;
        list = (JsonStringArrayList<Map<?, ?>>) listVector.getObject(rowId);
      } else {
        throw new RuntimeException("Unexpected map type:" + value.getClass().getName());
      }
      list.forEach(
          item -> {
            Object k =
                serialize(
                    item.get(KeyValueObjectInspector.KEY_FIELD_NAME),
                    moi.getMapKeyObjectInspector(),
                    -1 /* rowId will be ignored in this case */);
            Object v =
                serialize(
                    item.get(KeyValueObjectInspector.VALUE_FIELD_NAME),
                    moi.getMapValueObjectInspector(),
                    -1 /* rowId will be ignored in this case */);
            map.put(k, v);
          });
      return map;
    }

    if (objectInspector instanceof StructObjectInspector) { // Record/Struct type
      StructObjectInspector soi = (StructObjectInspector) objectInspector;
      JsonStringHashMap<?, ?> structData;
      if (value instanceof StructVector) {
        StructVector structVector = (StructVector) value;
        structData = (JsonStringHashMap<?, ?>) (structVector).getObject(rowId);
      } else if (value instanceof JsonStringHashMap) {
        structData = (JsonStringHashMap<?, ?>) value;
      } else {
        throw new RuntimeException("Unexpected struct type:" + value.getClass().getName());
      }
      int numFields = structData.size();
      Object[] row = new Object[numFields];
      int i = 0;
      for (Map.Entry<?, ?> entry : structData.entrySet()) {
        String fieldName = (String) entry.getKey();
        Object fieldValue = entry.getValue();
        row[i] =
            serialize(
                fieldValue,
                soi.getStructFieldRef(fieldName).getFieldObjectInspector(),
                -1 /* rowId will be ignored in this case */);
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
