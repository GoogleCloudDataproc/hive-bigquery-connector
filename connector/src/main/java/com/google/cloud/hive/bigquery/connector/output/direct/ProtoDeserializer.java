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

import static com.google.cloud.hive.bigquery.connector.utils.bq.BigQueryValueConverter.convertHiveValueToBigQuery;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.utils.hive.KeyValueObjectInspector;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.util.*;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

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
      ObjectInspector objectInspector,
      Descriptors.Descriptor nestedTypeDescriptor,
      Field bigqueryField,
      Object hiveValue) {
    if (hiveValue == null) {
      return null;
    }

    if (objectInspector instanceof ListObjectInspector) {
      ListObjectInspector loi = (ListObjectInspector) objectInspector;
      ObjectInspector elementObjectInspector = loi.getListElementObjectInspector();
      Iterator<?> iterator = loi.getList(hiveValue).iterator();
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

    if (objectInspector instanceof StructObjectInspector) {
      return buildSingleRowMessage(
          (StructObjectInspector) objectInspector,
          nestedTypeDescriptor,
          bigqueryField.getSubFields(),
          hiveValue);
    }

    // Convert Hive map to a list of BigQuery structs (proto messages)
    if (objectInspector instanceof MapObjectInspector) {
      MapObjectInspector moi = (MapObjectInspector) objectInspector;
      List<Object> list = new ArrayList<>();
      KeyValueObjectInspector kvoi = KeyValueObjectInspector.create(moi);
      for (Map.Entry<?, ?> entry : ((Map<?, ?>) hiveValue).entrySet()) {
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

    return convertHiveValueToBigQuery(
        objectInspector, hiveValue, HiveBigQueryConfig.WRITE_METHOD_DIRECT);
  }
}
