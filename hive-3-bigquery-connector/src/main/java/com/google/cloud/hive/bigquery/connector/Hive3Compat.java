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

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.input.udfs.*;
import com.google.cloud.hive.bigquery.connector.utils.DatetimeUtils;
import com.google.cloud.hive.bigquery.connector.utils.avro.AvroUtils;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.DescriptorProtos;
import java.time.*;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.*;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.serde2.io.*;
import org.apache.hadoop.hive.serde2.lazy.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampLocalTZObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TimestampLocalTZTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class Hive3Compat extends HiveCompat {

  private final Cache<Object, Object> cache = CacheBuilder.newBuilder().build();

  public Object convertHiveTimeUnitToBq(
      ObjectInspector objectInspector, Object hiveValue, String writeMethod) {
    if (objectInspector instanceof TimestampObjectInspector) {
      TimestampWritableV2 writable;
      if (hiveValue instanceof LazyTimestamp) {
        writable = ((LazyTimestamp) hiveValue).getWritableObject();
      } else {
        writable = (TimestampWritableV2) hiveValue;
      }
      Timestamp timestamp = writable.getTimestamp();
      if (writeMethod.equals(HiveBigQueryConfig.WRITE_METHOD_INDIRECT)) {
        return DatetimeUtils.getEpochMicrosFromHiveTimestamp(timestamp);
      } else {
        return DatetimeUtils.getEncodedProtoLongFromHiveTimestamp(timestamp);
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
      return DatetimeUtils.getEpochMicrosFromHiveTimestampTZ(timestampTZ);
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
    return null;
  }

  @Override
  public Object convertTimeUnitFromArrow(ObjectInspector objectInspector, Object value, int rowId) {
    if (objectInspector instanceof DateObjectInspector) {
      return new DateWritableV2(((DateDayVector) value).get(rowId));
    }
    if (objectInspector instanceof TimestampObjectInspector) {
      Timestamp timestamp =
          DatetimeUtils.getHiveTimestampFromLocalDatetime(
              ((TimeStampMicroVector) value).getObject(rowId));
      return new TimestampWritableV2(timestamp);
    }
    if (objectInspector instanceof TimestampLocalTZObjectInspector) {
      TimestampTZ timestampTZ =
          DatetimeUtils.getHiveTimestampTZFromUTC(((TimeStampMicroTZVector) value).get(rowId));
      return new TimestampLocalTZWritable(timestampTZ);
    }
    return null;
  }

  @Override
  public Object convertTimeUnitFromAvro(ObjectInspector objectInspector, Object value) {
    if (objectInspector instanceof DateObjectInspector) {
      return new DateWritableV2((int) value);
    }
    if (objectInspector instanceof TimestampObjectInspector) {
      LocalDateTime localDateTime = LocalDateTime.parse(((Utf8) value).toString());
      Timestamp timestamp = DatetimeUtils.getHiveTimestampFromLocalDatetime(localDateTime);
      TimestampWritableV2 timestampWritable = new TimestampWritableV2();
      timestampWritable.setInternal(timestamp.toEpochMilli(), timestamp.getNanos());
      return timestampWritable;
    }
    if (objectInspector instanceof TimestampLocalTZObjectInspector) {
      TimestampTZ timestampTZ = DatetimeUtils.getHiveTimestampTZFromUTC((long) value);
      return new TimestampLocalTZWritable(timestampTZ);
    }
    return null;
  }

  @Override
  public Schema getAvroSchema(ObjectInspector fieldOi, Field bigqueryField) {
    if (fieldOi instanceof TimestampLocalTZObjectInspector) {
      Schema schema = Schema.create(Schema.Type.LONG);
      schema.addProp("logicalType", "timestamp-micros");
      boolean nullable = bigqueryField.getMode() != Field.Mode.REQUIRED;
      return AvroUtils.nullableAvroSchema(schema, nullable);
    }
    return super.getAvroSchema(fieldOi, bigqueryField);
  }

  @Override
  public Map<PrimitiveObjectInspector.PrimitiveCategory, StandardSQLTypeName>
      getHiveToBqTypeMappings() {
    String cacheKey = "Hive3Compat.getHiveToBqTypeMappings";
    Map<PrimitiveObjectInspector.PrimitiveCategory, StandardSQLTypeName> cachedResult =
        (Map<PrimitiveObjectInspector.PrimitiveCategory, StandardSQLTypeName>)
            cache.getIfPresent(cacheKey);
    if (cachedResult != null) {
      return cachedResult;
    }
    Map<PrimitiveObjectInspector.PrimitiveCategory, StandardSQLTypeName> map =
        super.getHiveToBqTypeMappings();
    map.put(
        PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMPLOCALTZ, StandardSQLTypeName.TIMESTAMP);
    cache.put(cacheKey, map);
    return map;
  }

  @Override
  public Map<PrimitiveObjectInspector.PrimitiveCategory, DescriptorProtos.FieldDescriptorProto.Type>
      getHiveToProtoMappings() {
    String cacheKey = "Hive3Compat.getHiveToProtoMappings";
    Map<PrimitiveObjectInspector.PrimitiveCategory, DescriptorProtos.FieldDescriptorProto.Type>
        cachedResult =
            (Map<
                    PrimitiveObjectInspector.PrimitiveCategory,
                    DescriptorProtos.FieldDescriptorProto.Type>)
                cache.getIfPresent(cacheKey);
    if (cachedResult != null) {
      return cachedResult;
    }
    Map<PrimitiveObjectInspector.PrimitiveCategory, DescriptorProtos.FieldDescriptorProto.Type>
        map = super.getHiveToProtoMappings();
    map.put(
        PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMPLOCALTZ,
        DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64);
    cache.put(cacheKey, map);
    return map;
  }

  @Override
  public List<String> getUDFsRequiringExtraParentheses() {
    String cacheKey = "Hive3Compat.getUDFsRequiringExtraParentheses";
    List<String> cachedResult = (List<String>) cache.getIfPresent(cacheKey);
    if (cachedResult != null) {
      return cachedResult;
    }
    List<String> result = super.getUDFsRequiringExtraParentheses();
    for (Class udf :
        new Class[] {
          GenericUDFOPNull.class,
          GenericUDFOPNotNull.class,
          GenericUDFOPTrue.class,
          GenericUDFOPNotTrue.class,
          GenericUDFOPFalse.class,
          GenericUDFOPNotFalse.class,
        }) {
      result.add(udf.getName());
    }
    cache.put(cacheKey, result);
    return result;
  }

  @Override
  public GenericUDF convertUDF(ExprNodeGenericFuncDesc expr, Configuration conf) {
    GenericUDF udf = expr.getGenericUDF();
    if (udf instanceof GenericUDFToTimestampLocalTZ) {
      return new BigQueryUDFCastTimestamp();
    }
    if (udf instanceof UDFYear) {
      return new BigQueryUDFYear();
    }
    if (udf instanceof UDFMonth) {
      return new BigQueryUDFMonth();
    }
    if (udf instanceof UDFDayOfMonth) {
      return new BigQueryUDFDayOfMonth();
    }
    if (udf instanceof UDFHour) {
      return new BigQueryUDFHour();
    }
    if (udf instanceof UDFMinute) {
      return new BigQueryUDFMinute();
    }
    if (udf instanceof UDFSecond) {
      return new BigQueryUDFSecond();
    }
    return super.convertUDF(expr, conf);
  }

  @Override
  public String formatPredicateValue(TypeInfo typeInfo, Object value) {
    if (value != null && typeInfo instanceof TimestampLocalTZTypeInfo) {
      return "TIMESTAMP'" + value + "'";
    }
    return super.formatPredicateValue(typeInfo, value);
  }
}
