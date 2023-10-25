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
package com.google.cloud.hive.bigquery.connector;

import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.utils.DatetimeUtils;
import java.sql.Timestamp;
import java.time.*;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyDate;
import org.apache.hadoop.hive.serde2.lazy.LazyTimestamp;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;

public class Hive2Compat extends HiveCompat {

  @Override
  public Object convertHiveTimeUnitToBq(
      ObjectInspector objectInspector, Object hiveValue, String writeMethod) {
    if (objectInspector instanceof TimestampObjectInspector) {
      TimestampWritable writable;
      if (hiveValue instanceof LazyTimestamp) {
        writable = ((LazyTimestamp) hiveValue).getWritableObject();
      } else {
        writable = (TimestampWritable) hiveValue;
      }
      Timestamp timestamp = writable.getTimestamp();
      if (writeMethod.equals(HiveBigQueryConfig.WRITE_METHOD_INDIRECT)) {
        return DatetimeUtils.getEpochMicrosFromHiveTimestamp(timestamp);
      } else {
        return DatetimeUtils.getEncodedProtoLongFromHiveTimestamp(timestamp);
      }
    }
    if (objectInspector instanceof DateObjectInspector) {
      DateWritable writable;
      if (hiveValue instanceof LazyDate) {
        writable = ((LazyDate) hiveValue).getWritableObject();
      } else {
        writable = (DateWritable) hiveValue;
      }
      return new Integer(writable.getDays());
    }

    return null;
  }

  @Override
  public Object convertTimeUnitFromArrow(ObjectInspector objectInspector, Object value, int rowId) {
    if (objectInspector instanceof DateObjectInspector) {
      return new DateWritable(((DateDayVector) value).get(rowId));
    }
    if (objectInspector instanceof TimestampObjectInspector) {
      Timestamp timestamp =
          DatetimeUtils.getHiveTimestampFromLocalDatetime(
              ((TimeStampMicroVector) value).getObject(rowId));
      return new TimestampWritable(timestamp);
    }
    return null;
  }

  @Override
  public Object convertTimeUnitFromAvro(ObjectInspector objectInspector, Object value) {
    if (objectInspector instanceof DateObjectInspector) {
      return new DateWritable((int) value);
    }
    if (objectInspector instanceof TimestampObjectInspector) {
      LocalDateTime localDateTime = LocalDateTime.parse(((Utf8) value).toString());
      Timestamp timestamp = DatetimeUtils.getHiveTimestampFromLocalDatetime(localDateTime);
      TimestampWritable timestampWritable = new TimestampWritable();
      timestampWritable.setInternal(timestamp.getTime(), timestamp.getNanos());
      return timestampWritable;
    }
    return null;
  }
}
