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
package com.google.cloud.hive.bigquery.connector.utils.proto;

import com.google.cloud.hive.bigquery.connector.Constants;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import repackaged.by.hivebqconnector.com.google.protobuf.Descriptors;
import repackaged.by.hivebqconnector.com.google.protobuf.DynamicMessage;

public class ProtoDeserializer {

  /**
   * Converts the given Hive-serialized object into a Proto message, so it can later be written to a
   * BigQuery stream using the Storage Write API.
   */
  public static DynamicMessage buildSingleRowMessage(
      StructObjectInspector soi, Descriptors.Descriptor schemaDescriptor, Object record) {
    DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(schemaDescriptor);

    List<? extends StructField> allStructFieldRefs = soi.getAllStructFieldRefs();
    List<Object> structFieldsDataAsList = soi.getStructFieldsDataAsList(record);

    for (int fieldIndex = 0; fieldIndex < schemaDescriptor.getFields().size(); fieldIndex++) {
      int protoFieldNumber = fieldIndex + 1;

      Object hiveValue = structFieldsDataAsList.get(fieldIndex);
      ObjectInspector fieldObjectInspector =
          allStructFieldRefs.get(fieldIndex).getFieldObjectInspector();

      Descriptors.Descriptor nestedTypeDescriptor =
          schemaDescriptor.findNestedTypeByName(
              ProtoSchemaConverter.RESERVED_NESTED_TYPE_NAME + protoFieldNumber);
      Object protoValue =
          convertHiveValueToProtoRowValue(fieldObjectInspector, hiveValue, nestedTypeDescriptor);

      if (protoValue == null) {
        continue;
      }

      Descriptors.FieldDescriptor fieldDescriptor =
          schemaDescriptor.findFieldByNumber(protoFieldNumber);
      messageBuilder.setField(fieldDescriptor, protoValue);
    }

    return messageBuilder.build();
  }

  private static Object convertHiveValueToProtoRowValue(
      ObjectInspector fieldObjectInspector,
      Object fieldValue,
      Descriptors.Descriptor nestedTypeDescriptor) {
    if (fieldValue == null) {
      return null;
    }

    if (fieldObjectInspector instanceof ListObjectInspector) {
      ListObjectInspector loi = (ListObjectInspector) fieldObjectInspector;
      ObjectInspector elementObjectInspector = loi.getListElementObjectInspector();
      Iterator<?> iterator = loi.getList(fieldValue).iterator();
      List<Object> protoValue = new ArrayList<>();
      while (iterator.hasNext()) {
        Object elementValue = iterator.next();
        Object converted =
            convertHiveValueToProtoRowValue(
                elementObjectInspector, elementValue, nestedTypeDescriptor);
        if (converted == null) {
          continue;
        }
        protoValue.add(converted);
      }
      return protoValue;
    }

    if (fieldObjectInspector instanceof StructObjectInspector) {
      return buildSingleRowMessage(
          (StructObjectInspector) fieldObjectInspector, nestedTypeDescriptor, fieldValue);
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
      TimestampWritableV2 timestamp = (TimestampWritableV2) fieldValue;
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
      if (fieldValue instanceof byte[]) {
        return fieldValue;
      }
      BytesWritable bytes = (BytesWritable) fieldValue;
      // Resize the bytes' array to remove any unnecessary extra capacity it might have
      bytes.setCapacity(bytes.getLength());
      return bytes.getBytes();
    }

    if (fieldObjectInspector instanceof StringObjectInspector) {
      if (fieldValue instanceof String) {
        return fieldValue;
      }
      return fieldValue.toString();
    }

    if (fieldObjectInspector instanceof HiveDecimalObjectInspector) {
      if (fieldValue instanceof String) {
        return fieldValue;
      }
      HiveDecimalWritable decimal = (HiveDecimalWritable) fieldValue;
      return decimal.getHiveDecimal().bigDecimalValue().toPlainString();
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
