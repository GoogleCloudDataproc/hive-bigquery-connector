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

import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.utils.DateTimeUtils;
import java.nio.ByteBuffer;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;
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

  /** Converts the given Hive value into a format that can be written to BigQuery. */
  public static Object convertHiveValueToBigQuery(
      ObjectInspector objectInspector, Object hiveValue, String writeMethod) {
    if (objectInspector instanceof ByteObjectInspector) { // Tiny Int
      ByteWritable writable;
      if (hiveValue instanceof LazyByte) {
        writable = ((LazyByte) hiveValue).getWritableObject();
      } else {
        writable = (ByteWritable) hiveValue;
      }
      return new Long(writable.get());
    }

    if (objectInspector instanceof ShortObjectInspector) { // Small Int
      ShortWritable writable;
      if (hiveValue instanceof LazyShort) {
        writable = ((LazyShort) hiveValue).getWritableObject();
      } else {
        writable = (ShortWritable) hiveValue;
      }
      return new Long(writable.get());
    }

    if (objectInspector instanceof IntObjectInspector) { // Regular Int
      IntWritable writable;
      if (hiveValue instanceof LazyInteger) {
        writable = ((LazyInteger) hiveValue).getWritableObject();
      } else {
        writable = (IntWritable) hiveValue;
      }
      return new Long(writable.get());
    }

    if (objectInspector instanceof LongObjectInspector) { // Big Int
      LongWritable writable;
      if (hiveValue instanceof LazyLong) {
        writable = ((LazyLong) hiveValue).getWritableObject();
      } else {
        writable = (LongWritable) hiveValue;
      }
      return new Long(writable.get());
    }

    if (objectInspector instanceof TimestampObjectInspector) {
      TimestampWritableV2 writable;
      if (hiveValue instanceof LazyTimestamp) {
        writable = ((LazyTimestamp) hiveValue).getWritableObject();
      } else {
        writable = (TimestampWritableV2) hiveValue;
      }
      Timestamp timestamp = writable.getTimestamp();
      if (writeMethod.equals(HiveBigQueryConfig.WRITE_METHOD_INDIRECT)) {
        return DateTimeUtils.getEpochMicrosFromHiveTimestamp(timestamp);
      } else {
        return DateTimeUtils.getEncodedProtoLongFromHiveTimestamp(timestamp);
      }
    }

    if (objectInspector instanceof TimestampLocalTZObjectInspector) {
      TimestampLocalTZWritable writable;
      if (hiveValue instanceof LazyTimestampLocalTZ) {
        writable = ((LazyTimestampLocalTZ) hiveValue).getWritableObject();
      } else {
        writable = (TimestampLocalTZWritable) hiveValue;
      }
      TimestampTZ timestampTZ = writable.getTimestampTZ();
      return DateTimeUtils.getEpochMicrosFromHiveTimestampTZ(timestampTZ);
    }

    if (objectInspector instanceof DateObjectInspector) {
      DateWritableV2 writable;
      if (hiveValue instanceof LazyDate) {
        writable = ((LazyDate) hiveValue).getWritableObject();
      } else {
        writable = (DateWritableV2) hiveValue;
      }
      return new Integer(writable.getDays());
    }

    if (objectInspector instanceof FloatObjectInspector) {
      FloatWritable writable;
      if (hiveValue instanceof LazyFloat) {
        writable = ((LazyFloat) hiveValue).getWritableObject();
      } else {
        writable = (FloatWritable) hiveValue;
      }
      return new Double(writable.get());
    }

    if (objectInspector instanceof DoubleObjectInspector) {
      DoubleWritable writable;
      if (hiveValue instanceof LazyDouble) {
        writable = ((LazyDouble) hiveValue).getWritableObject();
      } else {
        writable = (DoubleWritable) hiveValue;
      }
      return new Double(writable.get());
    }

    if (objectInspector instanceof BooleanObjectInspector) {
      BooleanWritable writable;
      if (hiveValue instanceof LazyBoolean) {
        writable = ((LazyBoolean) hiveValue).getWritableObject();
      } else {
        writable = (BooleanWritable) hiveValue;
      }
      return new Boolean(writable.get());
    }

    if (objectInspector instanceof HiveCharObjectInspector
        || objectInspector instanceof HiveVarcharObjectInspector
        || objectInspector instanceof StringObjectInspector) {
      return hiveValue.toString();
    }

    if (objectInspector instanceof BinaryObjectInspector) {
      BytesWritable writable;
      if (hiveValue instanceof LazyBinary) {
        writable = ((LazyBinary) hiveValue).getWritableObject();
      } else {
        writable = (BytesWritable) hiveValue;
      }
      // Resize the bytes array to remove any unnecessary extra capacity it might have
      writable.setCapacity(writable.getLength());
      if (writeMethod.equals(HiveBigQueryConfig.WRITE_METHOD_INDIRECT)) {
        // Wrap into a ByteBuffer
        ByteBuffer buffer = ByteBuffer.wrap(writable.getBytes());
        return buffer.rewind();
      } else {
        return writable.getBytes();
      }
    }

    if (objectInspector instanceof HiveDecimalObjectInspector) {
      HiveDecimalWritable writable;
      if (hiveValue instanceof LazyHiveDecimal) {
        writable = ((LazyHiveDecimal) hiveValue).getWritableObject();
      } else {
        writable = (HiveDecimalWritable) hiveValue;
      }
      if (writeMethod.equals(HiveBigQueryConfig.WRITE_METHOD_INDIRECT)) {
        int scale = ((HiveDecimalObjectInspector) objectInspector).scale();
        byte[] bytes = writable.getHiveDecimal().bigIntegerBytesScaled(scale);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return buffer.rewind();
      } else {
        return writable.getHiveDecimal().bigDecimalValue().toPlainString();
      }
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
