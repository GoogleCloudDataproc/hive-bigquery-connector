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
import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import repackaged.by.hivebqconnector.com.google.common.base.Preconditions;
import repackaged.by.hivebqconnector.com.google.common.collect.ImmutableMap;
import repackaged.by.hivebqconnector.com.google.protobuf.DescriptorProtos;
import repackaged.by.hivebqconnector.com.google.protobuf.Descriptors;

/** Utilities to convert Hive schemas into Proto descriptors. */
public class ProtoSchemaConverter {

  public static final String RESERVED_NESTED_TYPE_NAME = "STRUCT";

  private static final ImmutableMap<PrimitiveCategory, DescriptorProtos.FieldDescriptorProto.Type>
      hiveToProtoTypes =
          new ImmutableMap.Builder<PrimitiveCategory, DescriptorProtos.FieldDescriptorProto.Type>()
              .put(PrimitiveCategory.STRING, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
              .put(PrimitiveCategory.LONG, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
              .put(PrimitiveCategory.DOUBLE, DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE)
              .put(
                  PrimitiveCategory.DECIMAL, DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
              .put(PrimitiveCategory.BOOLEAN, DescriptorProtos.FieldDescriptorProto.Type.TYPE_BOOL)
              .put(PrimitiveCategory.DATE, DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
              .put(
                  PrimitiveCategory.TIMESTAMP,
                  DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
              .put(PrimitiveCategory.BINARY, DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES)
              .build();

  public static Descriptors.Descriptor toDescriptor(StructObjectInspector soi)
      throws Descriptors.DescriptorValidationException {

    DescriptorProtos.DescriptorProto.Builder descriptorBuilder =
        DescriptorProtos.DescriptorProto.newBuilder().setName("Schema");

    int initialDepth = 0;
    DescriptorProtos.DescriptorProto descriptorProto =
        buildDescriptorProtoWithFields(
            descriptorBuilder, soi.getAllStructFieldRefs(), initialDepth);

    return createDescriptorFromProto(descriptorProto);
  }

  private static DescriptorProtos.DescriptorProto buildDescriptorProtoWithFields(
      DescriptorProtos.DescriptorProto.Builder descriptorBuilder,
      List<? extends StructField> fields,
      int depth) {
    Preconditions.checkArgument(
        depth < Constants.MAX_BIGQUERY_NESTED_DEPTH,
        "Avro Schema exceeds BigQuery maximum nesting depth.");
    int messageNumber = 1;
    for (StructField field : fields) {
      String fieldName = field.getFieldName();
      ObjectInspector fieldOi = field.getFieldObjectInspector();

      // TODO: See if there's any way to find out if the field is "NULL" or "NOT NULL".
      // For now, assuming all fields are OPTIONAL.
      DescriptorProtos.FieldDescriptorProto.Label fieldLabel =
          DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL;

      if (fieldOi instanceof ListObjectInspector) {
        /* DescriptorProtos.FieldDescriptorProto.Label elementLabel = arrayType.containsNull() ?
        DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL :
        DescriptorProtos.FieldDescriptorProto.Label.LABEL_REQUIRED; TODO: how to support null instances inside an array (repeated field) in BigQuery?*/
        fieldOi = ((ListObjectInspector) fieldOi).getListElementObjectInspector();
        fieldLabel = DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED;
      }

      DescriptorProtos.FieldDescriptorProto.Builder protoFieldBuilder;

      if (fieldOi instanceof StructObjectInspector) {
        // TODO: Maintain this as a reserved nested-type name, which no column can have.
        String nestedName = RESERVED_NESTED_TYPE_NAME + messageNumber;
        List<? extends StructField> subFields =
            ((StructObjectInspector) fieldOi).getAllStructFieldRefs();
        DescriptorProtos.DescriptorProto.Builder nestedFieldTypeBuilder =
            descriptorBuilder.addNestedTypeBuilder().setName(nestedName);
        buildDescriptorProtoWithFields(nestedFieldTypeBuilder, subFields, depth + 1);
        protoFieldBuilder =
            createProtoFieldBuilder(fieldName, fieldLabel, messageNumber).setTypeName(nestedName);
      } else {
        DescriptorProtos.FieldDescriptorProto.Type fieldType = toProtoFieldType(fieldOi);
        protoFieldBuilder =
            createProtoFieldBuilder(fieldName, fieldLabel, messageNumber, fieldType);
      }
      descriptorBuilder.addField(protoFieldBuilder);
      messageNumber++;
    }
    return descriptorBuilder.build();
  }

  private static DescriptorProtos.FieldDescriptorProto.Builder createProtoFieldBuilder(
      String fieldName, DescriptorProtos.FieldDescriptorProto.Label fieldLabel, int messageNumber) {
    return DescriptorProtos.FieldDescriptorProto.newBuilder()
        .setName(fieldName)
        .setLabel(fieldLabel)
        .setNumber(messageNumber);
  }

  protected static DescriptorProtos.FieldDescriptorProto.Builder createProtoFieldBuilder(
      String fieldName,
      DescriptorProtos.FieldDescriptorProto.Label fieldLabel,
      int messageNumber,
      DescriptorProtos.FieldDescriptorProto.Type fieldType) {
    return createProtoFieldBuilder(fieldName, fieldLabel, messageNumber).setType(fieldType);
  }

  private static DescriptorProtos.FieldDescriptorProto.Type toProtoFieldType(ObjectInspector oi) {
    if (oi instanceof MapObjectInspector) {
      throw new IllegalArgumentException(Constants.MAPTYPE_ERROR_MESSAGE);
    } else if (oi instanceof PrimitiveObjectInspector) {
      PrimitiveCategory category = ((PrimitiveObjectInspector) oi).getPrimitiveCategory();
      return Preconditions.checkNotNull(
          hiveToProtoTypes.get(category),
          new IllegalStateException("Unexpected type: " + category.name()));
    } else {
      throw new IllegalStateException("Unexpected type: " + oi.getCategory().name());
    }
  }

  private static Descriptors.Descriptor createDescriptorFromProto(
      DescriptorProtos.DescriptorProto descriptorProto)
      throws Descriptors.DescriptorValidationException {
    DescriptorProtos.FileDescriptorProto fileDescriptorProto =
        DescriptorProtos.FileDescriptorProto.newBuilder().addMessageType(descriptorProto).build();

    Descriptors.Descriptor descriptor =
        Descriptors.FileDescriptor.buildFrom(
                fileDescriptorProto, new Descriptors.FileDescriptor[] {})
            .getMessageTypes()
            .get(0);

    return descriptor;
  }
}
