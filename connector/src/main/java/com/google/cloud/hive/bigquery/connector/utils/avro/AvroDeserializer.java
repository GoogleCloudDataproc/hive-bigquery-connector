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
package com.google.cloud.hive.bigquery.connector.utils.avro;

import com.google.cloud.hive.bigquery.connector.Constants;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.*;
import org.codehaus.jackson.JsonNode;

public class AvroDeserializer {

  /**
   * Converts the given Hive-serialized object into an Avro record, so it can later be written to
   * GCS and then loaded into BigQuery via the File Load API.
   */
  public static Record buildSingleRecord(StructObjectInspector soi, Schema schema, Object object) {
    Record record = new Record(schema);
    List<? extends StructField> allStructFieldRefs = soi.getAllStructFieldRefs();
    List<Object> structFieldsDataAsList = soi.getStructFieldsDataAsList(object);
    for (int fieldIndex = 0; fieldIndex < schema.getFields().size(); fieldIndex++) {
      Object hiveValue = structFieldsDataAsList.get(fieldIndex);
      ObjectInspector fieldObjectInspector =
          allStructFieldRefs.get(fieldIndex).getFieldObjectInspector();
      String fieldName = allStructFieldRefs.get(fieldIndex).getFieldName();
      Schema fieldSchema = schema.getField(fieldName).schema();
      Object avroValue = convertHiveValueToAvroValue(fieldObjectInspector, hiveValue, fieldSchema);
      record.put(fieldIndex, avroValue);
    }
    return record;
  }

  private static Object convertHiveValueToAvroValue(
      ObjectInspector fieldObjectInspector, Object fieldValue, Schema fieldSchema) {
    if (fieldValue == null) {
      return null;
    }

    AvroSchemaInfo schemaInfo = AvroUtils.getSchemaInfo(fieldSchema);

    if (fieldObjectInspector instanceof ListObjectInspector) {
      ListObjectInspector loi = (ListObjectInspector) fieldObjectInspector;
      ObjectInspector elementObjectInspector = loi.getListElementObjectInspector();
      Iterator<?> iterator = loi.getList(fieldValue).iterator();
      Schema elementSchema = schemaInfo.getActualSchema().getElementType();
      List<Object> avroValue = new ArrayList<>();
      while (iterator.hasNext()) {
        Object elementValue = iterator.next();
        Object converted =
            convertHiveValueToAvroValue(elementObjectInspector, elementValue, elementSchema);
        avroValue.add(converted);
      }
      return avroValue;
    }

    if (fieldObjectInspector instanceof StructObjectInspector) {
      return buildSingleRecord(
          (StructObjectInspector) fieldObjectInspector, schemaInfo.getActualSchema(), fieldValue);
    }

    if (fieldObjectInspector instanceof LongObjectInspector) {
      if (fieldValue instanceof Long) {
        return fieldValue;
      }
      return ((LongWritable) fieldValue).get();
    }

    if (fieldObjectInspector instanceof TimestampObjectInspector) {
      if (fieldValue instanceof Long) {
        return fieldValue;
      }
      JsonNode logicalType = schemaInfo.getActualSchema().getJsonProp("logicalType");
      TimestampWritableV2 timestamp = (TimestampWritableV2) fieldValue;
      if (logicalType != null) {
        if (logicalType.asText().equals("timestamp-millis")) {
          return timestamp.getSeconds() * 1_000;
        }
      }
      return timestamp.getSeconds() * 1_000_000 + timestamp.getNanos() / 1000;
    }

    if (fieldObjectInspector instanceof DateObjectInspector) {
      if (fieldValue instanceof Integer) {
        return fieldValue;
      }
      return ((DateWritableV2) fieldValue).getDays();
    }

    if (fieldObjectInspector instanceof DoubleObjectInspector) {
      if (fieldValue instanceof Double) {
        return fieldValue;
      }
      return ((DoubleWritable) fieldValue).get();
    }

    if (fieldObjectInspector instanceof BooleanObjectInspector) {
      if (fieldValue instanceof Boolean) {
        return fieldValue;
      }
      return ((BooleanWritable) fieldValue).get();
    }

    if (fieldObjectInspector instanceof BinaryObjectInspector) {
      if (fieldValue instanceof Buffer) {
        return fieldValue;
      }
      BytesWritable bytes = (BytesWritable) fieldValue;
      // Resize the bytes' array to remove any unnecessary extra capacity it might have
      bytes.setCapacity(bytes.getLength());
      // Wrap into a ByteBuffer
      ByteBuffer buffer = ByteBuffer.wrap(bytes.getBytes());
      return buffer.rewind();
    }

    if (fieldObjectInspector instanceof HiveCharObjectInspector) {
      return fieldValue.toString();
    }

    if (fieldObjectInspector instanceof HiveVarcharObjectInspector) {
      return fieldValue.toString();
    }

    if (fieldObjectInspector instanceof StringObjectInspector) {
      return fieldValue.toString();
    }

    if (fieldObjectInspector instanceof HiveDecimalObjectInspector) {
      if (fieldValue instanceof Buffer) {
        return fieldValue;
      }
      HiveDecimal decimal = ((HiveDecimalWritable) fieldValue).getHiveDecimal();
      int scale = ((HiveDecimalObjectInspector) fieldObjectInspector).scale();
      byte[] bytes = decimal.bigIntegerBytesScaled(scale);
      ByteBuffer buffer = ByteBuffer.wrap(bytes);
      return buffer.rewind();
    }

    if (fieldObjectInspector instanceof MapObjectInspector) {
      throw new IllegalArgumentException(Constants.MAPTYPE_ERROR_MESSAGE);
    }

    String unsupportedCategory;
    if (fieldObjectInspector instanceof PrimitiveObjectInspector) {
      unsupportedCategory =
          ((PrimitiveObjectInspector) fieldObjectInspector).getPrimitiveCategory().name();
    } else {
      unsupportedCategory = fieldObjectInspector.getCategory().name();
    }

    throw new IllegalStateException("Unexpected type: " + unsupportedCategory);
  }
}
