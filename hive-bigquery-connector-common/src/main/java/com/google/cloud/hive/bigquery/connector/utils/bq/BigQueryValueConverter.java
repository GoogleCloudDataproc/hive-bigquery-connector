/*
 * Copyright 2023 Google Inc. All Rights Reserved.
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
package com.google.cloud.hive.bigquery.connector.utils.bq;

import com.google.cloud.hive.bigquery.connector.HiveCompat;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.*;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.*;

public class BigQueryValueConverter {

  // In Hive 1, HiveDecimal doesn't have a `bigIntegerBytesScaled()` method.
  // We use this static variable to identify if the method is available.
  private static boolean hasBigIntegerBytesScaled;

  static {
    try {
      Method method = HiveDecimal.class.getMethod("bigIntegerBytesScaled", int.class);
      hasBigIntegerBytesScaled = (method != null);
    } catch (NoSuchMethodException e) {
      hasBigIntegerBytesScaled = false;
    }
  }

  public static byte[] getBigIntegerBytesScaled(HiveDecimal hiveDecimal, int scale) {
    if (hasBigIntegerBytesScaled) {
      // Use bigIntegerBytesScaled if available
      return hiveDecimal.bigIntegerBytesScaled(scale);
    } else {
      // Alternative approach for older Hive versions
      BigDecimal decimal = hiveDecimal.bigDecimalValue();
      BigDecimal scaledDecimal = decimal.setScale(scale, BigDecimal.ROUND_HALF_UP);
      BigInteger bigInt = scaledDecimal.unscaledValue();
      return bigInt.toByteArray();
    }
  }

  /** Converts the given Hive value into a format that can be written to BigQuery. */
  public static Object convertHiveValueToBigQuery(
      ObjectInspector objectInspector, Object hiveValue, String writeMethod) {
    if (objectInspector instanceof ByteObjectInspector) { // Tiny Int
      if (hiveValue instanceof Byte) {
        return ((Byte) hiveValue).longValue();
      }
      if (hiveValue instanceof LazyByte) {
        return (long) ((LazyByte) hiveValue).getWritableObject().get();
      }
      return (long) ((ByteWritable) hiveValue).get();
    }

    if (objectInspector instanceof ShortObjectInspector) { // Small Int
      if (hiveValue instanceof Short) {
        return ((Short) hiveValue).longValue();
      }
      if (hiveValue instanceof LazyShort) {
        return (long) ((LazyShort) hiveValue).getWritableObject().get();
      }
      return (long) ((ShortWritable) hiveValue).get();
    }

    if (objectInspector instanceof IntObjectInspector) { // Regular Int
      if (hiveValue instanceof Integer) {
        return ((Integer) hiveValue).longValue();
      }
      if (hiveValue instanceof LazyInteger) {
        return (long) ((LazyInteger) hiveValue).getWritableObject().get();
      }
      return (long) ((IntWritable) hiveValue).get();
    }

    if (objectInspector instanceof LongObjectInspector) { // Big Int
      if (hiveValue instanceof Long) {
        return hiveValue;
      }
      if (hiveValue instanceof LazyLong) {
        return ((LazyLong) hiveValue).getWritableObject().get();
      }
      return ((LongWritable) hiveValue).get();
    }

    if (objectInspector instanceof FloatObjectInspector) {
      if (hiveValue instanceof Float) {
        return ((Float) hiveValue).doubleValue();
      }
      if (hiveValue instanceof LazyFloat) {
        return (double) ((LazyFloat) hiveValue).getWritableObject().get();
      }
      return (double) ((FloatWritable) hiveValue).get();
    }

    if (objectInspector instanceof DoubleObjectInspector) {
      if (hiveValue instanceof Double) {
        return hiveValue;
      }
      if (hiveValue instanceof LazyDouble) {
        return ((LazyDouble) hiveValue).getWritableObject().get();
      }
      return ((DoubleWritable) hiveValue).get();
    }

    if (objectInspector instanceof BooleanObjectInspector) {
      if (hiveValue instanceof Boolean) {
        return hiveValue;
      }
      if (hiveValue instanceof LazyBoolean) {
        return ((LazyBoolean) hiveValue).getWritableObject().get();
      }
      return ((BooleanWritable) hiveValue).get();
    }

    if (objectInspector instanceof HiveCharObjectInspector
        || objectInspector instanceof HiveVarcharObjectInspector
        || objectInspector instanceof StringObjectInspector) {
      return hiveValue.toString();
    }

    if (objectInspector instanceof BinaryObjectInspector) {
      byte[] bytes;
      if (hiveValue instanceof byte[]) {
        bytes = (byte[]) hiveValue;
      } else if (hiveValue instanceof LazyBinary) {
        BytesWritable writable = ((LazyBinary) hiveValue).getWritableObject();
        writable.setCapacity(writable.getLength());
        bytes = writable.getBytes();
      } else {
        BytesWritable writable = ((BytesWritable) hiveValue);
        writable.setCapacity(writable.getLength());
        bytes = writable.getBytes();
      }
      if (writeMethod.equals(HiveBigQueryConfig.WRITE_METHOD_INDIRECT)) {
        // Wrap into a ByteBuffer for Avro writer
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return buffer.rewind();
      } else {
        return bytes;
      }
    }

    if (objectInspector instanceof HiveDecimalObjectInspector) {
      HiveDecimal hiveDecimal;
      if (hiveValue instanceof HiveDecimal) {
        hiveDecimal = (HiveDecimal) hiveValue;
      } else if (hiveValue instanceof LazyHiveDecimal) {
        hiveDecimal = ((LazyHiveDecimal) hiveValue).getWritableObject().getHiveDecimal();
      } else {
        hiveDecimal = ((HiveDecimalWritable) hiveValue).getHiveDecimal();
      }
      if (writeMethod.equals(HiveBigQueryConfig.WRITE_METHOD_INDIRECT)) {
        int scale = ((HiveDecimalObjectInspector) objectInspector).scale();
        byte[] bytes = getBigIntegerBytesScaled(hiveDecimal, scale);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return buffer.rewind();
      }
      return hiveDecimal.bigDecimalValue().toPlainString();
    }

    Object timeUnitConverted =
        HiveCompat.getInstance().convertHiveTimeUnitToBq(objectInspector, hiveValue, writeMethod);
    if (timeUnitConverted != null) {
      return timeUnitConverted;
    }

    String unsupportedCategory;
    if (objectInspector instanceof PrimitiveObjectInspector) {
      unsupportedCategory =
          ((PrimitiveObjectInspector) objectInspector).getPrimitiveCategory().name();
    } else {
      unsupportedCategory = objectInspector.getCategory().name();
    }

    throw new IllegalStateException("Unexpected type: " + unsupportedCategory);
  }
}
