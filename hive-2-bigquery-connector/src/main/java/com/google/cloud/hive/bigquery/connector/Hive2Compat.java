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
import com.google.cloud.hive.bigquery.connector.input.udfs.BigQueryUDFQuarter;
import com.google.cloud.hive.bigquery.connector.input.udfs.BigQueryUDFRegExpContains;
import com.google.cloud.hive.bigquery.connector.utils.DatetimeUtils;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCharacterLength;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLength;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFNullif;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOctetLength;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFQuarter;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFRegExp;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyDate;
import org.apache.hadoop.hive.serde2.lazy.LazyTimestamp;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Hive2Compat extends HiveCompat {

  private static final Logger LOG = LoggerFactory.getLogger(Hive2Compat.class);

  @Override
  public Object convertHiveTimeUnitToBq(
      ObjectInspector objectInspector, Object hiveValue, String writeMethod) {
    if (objectInspector instanceof TimestampObjectInspector) {
      Timestamp timestamp;
      if (hiveValue instanceof Timestamp) {
        timestamp = (Timestamp) hiveValue;
      } else if (hiveValue instanceof LazyTimestamp) {
        timestamp = ((LazyTimestamp) hiveValue).getWritableObject().getTimestamp();
      } else {
        timestamp = ((TimestampWritable) hiveValue).getTimestamp();
      }
      if (writeMethod.equals(HiveBigQueryConfig.WRITE_METHOD_INDIRECT)) {
        return DatetimeUtils.getEpochMicrosFromHiveTimestamp(timestamp);
      }
      return DatetimeUtils.getEncodedProtoLongFromHiveTimestamp(timestamp);
    }
    if (objectInspector instanceof DateObjectInspector) {
      if (hiveValue instanceof Date) {
        return (int) ((Date) hiveValue).toLocalDate().toEpochDay();
      }
      if (hiveValue instanceof LazyDate) {
        return (int) ((LazyDate) hiveValue).getWritableObject().get().toLocalDate().toEpochDay();
      }
      return ((DateWritable) hiveValue).getDays();
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

  @Override
  public GenericUDF convertUDF(ExprNodeGenericFuncDesc expr, Configuration conf) {
    GenericUDF udf = expr.getGenericUDF();
    if (udf instanceof GenericUDFQuarter) {
      return new BigQueryUDFQuarter();
    }
    if (udf instanceof GenericUDFRegExp) {
      return new BigQueryUDFRegExpContains();
    }
    return super.convertUDF(expr, conf);
  }

  @Override
  protected List<String> getIdenticalUDFs() {
    String cacheKey = "Hive2Compat.getIdenticalUDFs";
    List<String> cachedResult = (List<String>) cache.getIfPresent(cacheKey);
    if (cachedResult != null) {
      return cachedResult;
    }
    List<String> udfs = new ArrayList<>(super.getIdenticalUDFs());
    for (Class udf :
        new Class[] {
          GenericUDFNullif.class,
          GenericUDFLength.class,
          GenericUDFCharacterLength.class,
          GenericUDFOctetLength.class
        }) {
      udfs.add(udf.getName());
    }
    cache.put(cacheKey, udfs);
    return udfs;
  }

  @Override
  public ExprNodeGenericFuncDesc deserializeExpression(String s) {
    return SerializationUtilities.deserializeExpression(s);
  }
}
