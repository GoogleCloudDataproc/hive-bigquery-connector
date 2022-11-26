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
package com.google.cloud.hive.bigquery.connector.utils.arrow;

import com.google.cloud.hive.bigquery.connector.utils.hive.KeyValueObjectInspector;
import java.math.BigDecimal;
import java.time.*;
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
        floatValue = (float) value;
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
        LocalDateTime date = LocalDateTime.ofInstant(Instant.ofEpochMilli(secondsAsMillis), ZoneId.systemDefault());
        timestamp.setInternal(date.atZone(ZoneOffset.UTC).toInstant().toEpochMilli(), date.getNano());
        ///timestamp.setInternal(secondsAsMillis,nanos);
        return timestamp;
      }
      if (value instanceof TimeStampMicroVector) {
        LocalDateTime localDateTime = ((TimeStampMicroVector) value).getObject(rowId);
        TimestampWritableV2 timestamp = new TimestampWritableV2();
        timestamp.setInternal(localDateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli(), localDateTime.getNano());
        return timestamp;
      }
      // TODO: 'else' case
    }

    if (objectInspector instanceof ListObjectInspector) { // Array/List type
      if (value instanceof ListVector) {
        ListVector listVector = (ListVector) value;
        ListObjectInspector loi = (ListObjectInspector) objectInspector;
        int numItems = listVector.getDataVector().getValueCount();
        Object[] children = new Object[numItems];
        for (int i = 0; i < numItems; i++) {
          children[i] =
              serialize(listVector.getDataVector(), loi.getListElementObjectInspector(), i);
        }
        return children;
      }
      // TODO: 'else' case
    }

    if (objectInspector instanceof MapObjectInspector) { // Map type
      MapObjectInspector moi = (MapObjectInspector) objectInspector;
      Map<Object, Object> map = new HashMap<>();
      List<Map<?, ?>> list;
      if (value instanceof List) {
        list = (List<Map<?, ?>>) value;
      } else if (value instanceof ListVector) {
        ListVector listVector = (ListVector) value;
        list = (List<Map<?, ?>>) listVector.getObject(rowId);
      } else {
        throw new RuntimeException("Unexpected map type:" + value.getClass().getName());
      }
      list.forEach(
          item -> {
            Object k =
                serialize(
                    item.get(KeyValueObjectInspector.KEY_FIELD_NAME),
                    moi.getMapKeyObjectInspector(),
                    0);
            Object v =
                serialize(
                    item.get(KeyValueObjectInspector.VALUE_FIELD_NAME),
                    moi.getMapValueObjectInspector(),
                    0);
            map.put(k, v);
          });
      return map;
    }

    if (objectInspector instanceof StructObjectInspector) { // Record/Struct type
      if (value instanceof StructVector) {
        StructVector structVector = (StructVector) value;
        JsonStringHashMap<?, ?> structData =
            (JsonStringHashMap<?, ?>) (structVector).getObject(rowId);
        List<FieldVector> fieldVectors = structVector.getChildrenFromFields();
        int numFields = structVector.size();
        StructObjectInspector soi = (StructObjectInspector) objectInspector;
        Object[] row = new Object[numFields];
        int i = 0;
        for (Object v : structData.values()) {
          FieldVector fieldVector = fieldVectors.get(i);
          row[i] =
              serialize(
                  v, soi.getStructFieldRef(fieldVector.getName()).getFieldObjectInspector(), 0);
          i++;
        }
        return row;
      }
      // TODO: 'else' case
    }
    throw new UnsupportedOperationException(
        "Unsupported ObjectInspector `"
            + objectInspector.getClass().getName()
            + "` for value of type `"
            + value.getClass().getName()
            + "`");
  }
}
