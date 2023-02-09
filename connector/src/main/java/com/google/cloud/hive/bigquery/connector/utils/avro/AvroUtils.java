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

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.hive.bigquery.connector.utils.hive.KeyValueObjectInspector;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.mapred.JobConf;
import org.codehaus.jackson.node.IntNode;

public class AvroUtils {

  public static Schema getAvroSchema(StructObjectInspector soi, FieldList bigqueryFields) {
    List<Schema.Field> avroFields = new ArrayList<>();
    List<? extends StructField> allStructFieldRefs = soi.getAllStructFieldRefs();
    for (int i = 0; i < allStructFieldRefs.size(); i++) {
      StructField structField = allStructFieldRefs.get(i);
      Schema fieldSchema =
          getAvroSchema(structField.getFieldObjectInspector(), bigqueryFields.get(i));
      Schema.Field avroField =
          new Schema.Field(structField.getFieldName(), fieldSchema, null, null);
      avroFields.add(avroField);
    }
    Schema recordSchema =
        Schema.createRecord(
            "record_" + UUID.randomUUID().toString().replace("-", ""), null, null, false);
    recordSchema.setFields(avroFields);
    return recordSchema;
  }

  public static Schema getAvroSchema(ObjectInspector fieldOi, Field bigqueryField) {
    if (fieldOi instanceof ListObjectInspector) {
      ListObjectInspector loi = (ListObjectInspector) fieldOi;
      ObjectInspector elementOi = loi.getListElementObjectInspector();
      return Schema.createArray(getAvroSchema(elementOi, bigqueryField));
    }
    if (fieldOi instanceof StructObjectInspector) {
      return getAvroSchema((StructObjectInspector) fieldOi, bigqueryField.getSubFields());
    }
    if (fieldOi instanceof MapObjectInspector) {
      // Convert the Map type into a list of key/value records
      MapObjectInspector moi = (MapObjectInspector) fieldOi;
      Schema keySchema = Schema.create(Schema.Type.STRING);
      Schema.Field keyField =
          new Schema.Field(KeyValueObjectInspector.KEY_FIELD_NAME, keySchema, null, null);
      Schema valueSchema = getAvroSchema(moi.getMapValueObjectInspector(), bigqueryField);
      Schema.Field valueField =
          new Schema.Field(KeyValueObjectInspector.VALUE_FIELD_NAME, valueSchema, null, null);
      Schema entrySchema =
          Schema.createRecord(
              "map_" + UUID.randomUUID().toString().replace("-", ""), null, null, false);
      entrySchema.setFields(Arrays.asList(keyField, valueField));
      return Schema.createArray(entrySchema);
    }
    if (fieldOi instanceof ByteObjectInspector
        || fieldOi instanceof ShortObjectInspector
        || fieldOi instanceof IntObjectInspector) {
      return Schema.create(Schema.Type.INT);
    }
    if (fieldOi instanceof LongObjectInspector) {
      return Schema.create(Schema.Type.LONG);
    }
    if (fieldOi instanceof TimestampObjectInspector) {
      Schema schema = Schema.create(Schema.Type.LONG);
      if (bigqueryField.getType().getStandardType().equals(StandardSQLTypeName.TIMESTAMP)) {
        schema.addProp("logicalType", "timestamp-micros");
      } else if (bigqueryField.getType().getStandardType().equals(StandardSQLTypeName.DATETIME)) {
        schema.addProp("logicalType", "local-timestamp-micros");
      } else {
        throw new RuntimeException(
            String.format(
                "Unexpected BigQuery type `%s` for field `%s` with Hive type `%s`",
                bigqueryField.getType().getStandardType(),
                bigqueryField.getName(),
                fieldOi.getTypeName()));
      }
      return schema;
    }
    if (fieldOi instanceof TimestampLocalTZObjectInspector) {
      Schema schema = Schema.create(Schema.Type.LONG);
      schema.addProp("logicalType", "timestamp-micros");
      return schema;
    }
    if (fieldOi instanceof DateObjectInspector) {
      Schema schema = Schema.create(Schema.Type.INT);
      schema.addProp("logicalType", "date");
      return schema;
    }
    if (fieldOi instanceof FloatObjectInspector) {
      return Schema.create(Schema.Type.FLOAT);
    }
    if (fieldOi instanceof DoubleObjectInspector) {
      return Schema.create(Schema.Type.DOUBLE);
    }
    if (fieldOi instanceof BooleanObjectInspector) {
      return Schema.create(Schema.Type.BOOLEAN);
    }
    if (fieldOi instanceof BinaryObjectInspector) {
      return Schema.create(Schema.Type.BYTES);
    }
    if (fieldOi instanceof HiveCharObjectInspector
        || fieldOi instanceof HiveVarcharObjectInspector
        || fieldOi instanceof StringObjectInspector) {
      return Schema.create(Schema.Type.STRING);
    }
    if (fieldOi instanceof HiveDecimalObjectInspector) {
      HiveDecimalObjectInspector hdoi = (HiveDecimalObjectInspector) fieldOi;
      Schema schema = Schema.create(Schema.Type.BYTES);
      schema.addProp("logicalType", "decimal");
      schema.addProp("precision", IntNode.valueOf(hdoi.precision()));
      schema.addProp("scale", IntNode.valueOf(hdoi.scale()));
      return schema;
    }

    String unsupportedCategory;
    if (fieldOi instanceof PrimitiveObjectInspector) {
      unsupportedCategory = ((PrimitiveObjectInspector) fieldOi).getPrimitiveCategory().name();
    } else {
      unsupportedCategory = fieldOi.getCategory().name();
    }

    throw new IllegalStateException("Unexpected type: " + unsupportedCategory);
  }

  /**
   * This function is used primarily to deal with UNION type objects, which are a union of two
   * components: "null" (if the type is nullable) and a primitive Avro type. This function
   * essentially decouples and returns those two components.
   */
  public static AvroSchemaInfo getSchemaInfo(Schema fieldSchema) {
    AvroSchemaInfo schemaInfo = new AvroSchemaInfo(fieldSchema, false);
    // Check if field is nullable, which is represented as an UNION of NULL and primitive type.
    if (schemaInfo.getActualSchema().getType() == Schema.Type.UNION) {
      if (fieldSchema.getTypes().size() == 2) {
        if (fieldSchema.getTypes().get(0).getType() == Schema.Type.NULL) {
          schemaInfo.setNullable(true);
          schemaInfo.setActualSchema(fieldSchema.getTypes().get(1));
        } else if (fieldSchema.getTypes().get(1).getType() == Schema.Type.NULL) {
          schemaInfo.setNullable(true);
          schemaInfo.setActualSchema(fieldSchema.getTypes().get(0));
        } else {
          throw new RuntimeException("Unexpected type: " + fieldSchema);
        }
      } else {
        throw new RuntimeException("Unexpected type: " + fieldSchema);
      }
    }
    return schemaInfo;
  }

  /**
   * Helper class that writes Avro records into memory. This is used by the 'indirect' write method.
   * The contents are later persisted to GCS when each task completes. And later written to BigQuery
   * when the overall job completes.
   */
  public static class AvroOutput {

    final ByteArrayOutputStream outputStream;
    final DataFileWriter<GenericRecord> dataFileWriter;

    public AvroOutput(
        DataFileWriter<GenericRecord> dataFileWriter, ByteArrayOutputStream outputStream) {
      this.dataFileWriter = dataFileWriter;
      this.outputStream = outputStream;
    }

    public DataFileWriter<GenericRecord> getDataFileWriter() {
      return dataFileWriter;
    }

    public ByteArrayOutputStream getOutputStream() {
      return outputStream;
    }

    public static AvroOutput initialize(JobConf jobConf, Schema schema) {
      GenericDatumWriter<GenericRecord> gdw = new GenericDatumWriter<>(schema);
      DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(gdw);
      int level = jobConf.getInt(AvroOutputFormat.DEFLATE_LEVEL_KEY, -1);
      String codecName = jobConf.get(AvroJob.OUTPUT_CODEC, "deflate");
      CodecFactory factory =
          codecName.equals("deflate")
              ? CodecFactory.deflateCodec(level)
              : CodecFactory.fromString(codecName);
      dataFileWriter.setCodec(factory);
      dataFileWriter.setMeta("writer.time.zone", TimeZone.getDefault().toZoneId().toString());
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try {
        dataFileWriter.create(schema, baos);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return new AvroOutput(dataFileWriter, baos);
    }
  }
}
