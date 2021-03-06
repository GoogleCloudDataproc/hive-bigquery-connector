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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.io.*;
import repackaged.by.hivebqconnector.org.apache.arrow.vector.*;
import repackaged.by.hivebqconnector.org.apache.arrow.vector.complex.ListVector;
import repackaged.by.hivebqconnector.org.apache.arrow.vector.complex.StructVector;

public class ArrowSerializer {

  /** Converts the given Arrow-formatted value to a serialized format that Hive understands. */
  public static Object serializeVector(ValueVector vector, int rowId) {
    if (vector.isNull(rowId)) {
      return null;
    }
    if (vector instanceof BitVector) {
      return new BooleanWritable(((BitVector) vector).get(rowId) == 1);
    } else if (vector instanceof BigIntVector) {
      return new LongWritable(((BigIntVector) vector).get(rowId));
    } else if (vector instanceof Float8Vector) {
      return new DoubleWritable(((Float8Vector) vector).get(rowId));
    } else if (vector instanceof DecimalVector) {
      DecimalVector v = (DecimalVector) vector;
      HiveDecimal hiveDecimal = HiveDecimal.create(v.getObject(rowId));
      HiveDecimal.enforcePrecisionScale(hiveDecimal, v.getPrecision(), v.getScale());
      return new HiveDecimalWritable(hiveDecimal);
    } else if (vector instanceof VarCharVector) {
      VarCharVector v = (VarCharVector) vector;
      if (v.isSet(rowId) == 0) {
        return null;
      } else {
        return new Text(v.getObject(rowId).toString());
      }
    } else if (vector instanceof VarBinaryVector) {
      return new BytesWritable(((VarBinaryVector) vector).getObject(rowId));
    } else if (vector instanceof DateDayVector) {
      int intValue = ((DateDayVector) vector).get(rowId);
      LocalDate localDate = LocalDate.ofEpochDay(intValue);
      Date date = new Date();
      date.setDayOfMonth(localDate.getDayOfMonth());
      date.setMonth(localDate.getMonth().getValue());
      date.setYear(localDate.getYear());
      return new DateWritableV2(date);
    } else if (vector instanceof TimeStampMicroVector) {
      LocalDateTime localDateTime = ((TimeStampMicroVector) vector).getObject(rowId);
      TimestampWritableV2 timestamp = new TimestampWritableV2();
      timestamp.setInternal(localDateTime.toEpochSecond(ZoneOffset.UTC), localDateTime.getNano());
      return timestamp;
    } else if (vector instanceof TimeStampMicroTZVector) {
      long longValue = ((TimeStampMicroTZVector) vector).get(rowId);
      TimestampWritableV2 timestamp = new TimestampWritableV2();
      long secondsAsMillis = (longValue / 1_000_000) * 1_000;
      int nanos = (int) (longValue % 1_000_000) * 1_000;
      timestamp.setInternal(secondsAsMillis, nanos);
      return timestamp;
    } else if (vector instanceof ListVector) {
      ListVector listVector = (ListVector) vector;
      int numItems = listVector.getDataVector().getValueCount();
      Object[] children = new Object[numItems];
      for (int i = 0; i < numItems; i++) {
        children[i] = serializeVector(listVector.getDataVector(), i);
      }
      return children;
    } else if (vector instanceof StructVector) {
      StructVector structVector = (StructVector) vector;
      int numItems = structVector.size();
      List<FieldVector> childrenVectors = structVector.getChildrenFromFields();
      Object[] children = new Object[numItems];
      for (int i = 0; i < numItems; i++) {
        FieldVector childVector = childrenVectors.get(i);
        children[i] = serializeVector(childVector, 0);
      }
      return children;
    } else {
      throw new UnsupportedOperationException(
          "Unsupported Arrow vector type: " + vector.getClass().getName());
    }
  }
}
