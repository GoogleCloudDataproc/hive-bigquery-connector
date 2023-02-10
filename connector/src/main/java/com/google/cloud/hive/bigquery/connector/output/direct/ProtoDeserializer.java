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
package com.google.cloud.hive.bigquery.connector.output.direct;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.hive.bigquery.connector.utils.DateTimeUtils;
import com.google.cloud.hive.bigquery.connector.utils.hive.KeyValueObjectInspector;
import java.util.*;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.serde2.io.*;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.*;
import repackaged.by.hivebqconnector.com.google.protobuf.Descriptors;
import repackaged.by.hivebqconnector.com.google.protobuf.DynamicMessage;

public class ProtoDeserializer {

  /**
   * Converts the given Hive-serialized object into a Proto message, so it can later be written to a
   * BigQuery stream using the Storage Write API.
   */
  public static DynamicMessage buildSingleRowMessage(
      StructObjectInspector soi,
      Descriptors.Descriptor protoDescriptor,
      FieldList bigqueryFields,
      Object record) {
    DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(protoDescriptor);
    List<? extends StructField> allStructFieldRefs = soi.getAllStructFieldRefs();
    List<Object> structFieldsDataAsList = soi.getStructFieldsDataAsList(record);
    for (int fieldIndex = 0; fieldIndex < protoDescriptor.getFields().size(); fieldIndex++) {
      int protoFieldNumber = fieldIndex + 1;
      Object hiveValue = structFieldsDataAsList.get(fieldIndex);
      ObjectInspector fieldObjectInspector =
          allStructFieldRefs.get(fieldIndex).getFieldObjectInspector();
      Field bigqueryField = bigqueryFields.get(fieldIndex);
      Descriptors.Descriptor nestedTypeDescriptor =
          protoDescriptor.findNestedTypeByName(
              ProtoSchemaConverter.RESERVED_NESTED_TYPE_NAME + protoFieldNumber);
      Object protoValue =
          convertHiveValueToProtoRowValue(
              fieldObjectInspector, nestedTypeDescriptor, bigqueryField, hiveValue);
      if (protoValue == null) {
        continue;
      }
      Descriptors.FieldDescriptor fieldDescriptor =
          protoDescriptor.findFieldByNumber(protoFieldNumber);
      messageBuilder.setField(fieldDescriptor, protoValue);
    }
    return messageBuilder.build();
  }

  private static Object convertHiveValueToProtoRowValue(
      ObjectInspector fieldObjectInspector,
      Descriptors.Descriptor nestedTypeDescriptor,
      Field bigqueryField,
      Object fieldValue) {
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
                elementObjectInspector, nestedTypeDescriptor, bigqueryField, elementValue);
        if (converted == null) {
          continue;
        }
        protoValue.add(converted);
      }
      return protoValue;
    }

    if (fieldObjectInspector instanceof StructObjectInspector) {
      return buildSingleRowMessage(
          (StructObjectInspector) fieldObjectInspector,
          nestedTypeDescriptor,
          bigqueryField.getSubFields(),
          fieldValue);
    }

    // Convert Hive map to a list of BigQuery structs (proto messages)
    if (fieldObjectInspector instanceof MapObjectInspector) {
      MapObjectInspector moi = (MapObjectInspector) fieldObjectInspector;
      List<Object> list = new ArrayList<>();
      KeyValueObjectInspector kvoi = KeyValueObjectInspector.create(moi);
      for (Map.Entry<?, ?> entry : ((Map<?, ?>) fieldValue).entrySet()) {
        DynamicMessage entryMessage =
            buildSingleRowMessage(
                kvoi,
                nestedTypeDescriptor,
                bigqueryField.getSubFields(),
                Arrays.asList(entry.getKey(), entry.getValue()));
        list.add(entryMessage);
      }
      return list;
    }

    if (fieldObjectInspector instanceof ByteObjectInspector) { // Tiny Int
      return (long) ((ByteWritable) fieldValue).get();
    }

    if (fieldObjectInspector instanceof ShortObjectInspector) { // Small Int
      return (long) ((ShortWritable) fieldValue).get();
    }

    if (fieldObjectInspector instanceof IntObjectInspector) { // Regular Int
      return (long) ((IntWritable) fieldValue).get();
    }

    if (fieldObjectInspector instanceof LongObjectInspector) { // Big Int
      return ((LongWritable) fieldValue).get();
    }

    if (fieldObjectInspector instanceof TimestampObjectInspector) {
      Timestamp timestamp = ((TimestampWritableV2) fieldValue).getTimestamp();
      return DateTimeUtils.getEncodedProtoLongFromHiveTimestamp(timestamp);
    }

    if (fieldObjectInspector instanceof TimestampLocalTZObjectInspector) {
      TimestampTZ timestampTZ = ((TimestampLocalTZWritable) fieldValue).getTimestampTZ();
      return DateTimeUtils.getEpochMicrosFromHiveTimestampTZ(timestampTZ);
    }

    if (fieldObjectInspector instanceof DateObjectInspector) {
      return ((DateWritableV2) fieldValue).getDays();
    }

    if (fieldObjectInspector instanceof FloatObjectInspector) {
      return Double.valueOf(Float.toString(((FloatWritable) fieldValue).get()));
    }

    if (fieldObjectInspector instanceof DoubleObjectInspector) {
      return ((DoubleWritable) fieldValue).get();
    }

    if (fieldObjectInspector instanceof BooleanObjectInspector) {
      return ((BooleanWritable) fieldValue).get();
    }

    if (fieldObjectInspector instanceof BinaryObjectInspector) {
      BytesWritable bytes = (BytesWritable) fieldValue;
      // Resize the bytes' array to remove any unnecessary extra capacity it might have
      bytes.setCapacity(bytes.getLength());
      return bytes.getBytes();
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
      HiveDecimalWritable decimal = (HiveDecimalWritable) fieldValue;
      return decimal.getHiveDecimal().bigDecimalValue().toPlainString();
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
