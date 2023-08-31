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
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.hive.bigquery.connector.JobDetails;
import com.google.cloud.hive.bigquery.connector.utils.hive.KeyValueObjectInspector;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.mapred.JobConf;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

public class AvroUtils {

  /**
   * Variable used to figure out if the user has an old or newer version of the
   * Avro library. This is used to get around some changes in the API of the
   * library. For example, the `getJsonProp()` method was public in old versions and
   * then made private in newer versions in favor or a new `getObjectProp()` method.
   */
  public static Boolean usesOldAvroLib;

  static {
    try {
      Schema emptySchema = SchemaBuilder.builder().nullType();
      emptySchema.getClass().getMethod("getJsonProp", String.class);
      usesOldAvroLib = true;
    } catch (NoSuchMethodException e) {
      usesOldAvroLib = false;
    }
  }

  public static void addProp(Schema schema, String propName, String propValue) {
    schema.addProp(propName, propValue);
  }

  public static void addProp(Schema schema, String propName, Object propValue) {
    if (usesOldAvroLib) {
      ObjectMapper objectMapper = new ObjectMapper();
      schema.addProp(propName, objectMapper.convertValue(propValue, JsonNode.class));
    } else {
      schema.addProp(propName, propValue);
    }
  }

  public static int getPropAsInt(Schema schema, String propName) {
    try {
      if (usesOldAvroLib) {
        Method method = schema.getClass().getMethod("getJsonProp", String.class);
        JsonNode value = (JsonNode) method.invoke(schema, propName);
        return value.asInt();
      }
      Method method = schema.getClass().getMethod("getObjectProp", String.class);
      return (int) method.invoke(schema, propName);
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

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

  /* Returns a nullable schema if the field is nullable */
  private static Schema modedAvroSchema(Schema fieldSchema, boolean nullable) {
    return nullable
        ? Schema.createUnion(Arrays.asList(fieldSchema, Schema.create(Schema.Type.NULL)))
        : fieldSchema;
  }

  public static Schema getAvroSchema(ObjectInspector fieldOi, Field bigqueryField) {
    boolean nullable = bigqueryField.getMode() != Mode.REQUIRED;
    if (fieldOi instanceof ListObjectInspector) {
      ListObjectInspector loi = (ListObjectInspector) fieldOi;
      ObjectInspector elementOi = loi.getListElementObjectInspector();
      return modedAvroSchema(Schema.createArray(getAvroSchema(elementOi, bigqueryField)), nullable);
    }
    if (fieldOi instanceof StructObjectInspector) {
      return modedAvroSchema(
          getAvroSchema((StructObjectInspector) fieldOi, bigqueryField.getSubFields()), nullable);
    }
    if (fieldOi instanceof MapObjectInspector) {
      // Convert the Map type into a list of key/value records
      MapObjectInspector moi = (MapObjectInspector) fieldOi;
      Schema keySchema = Schema.create(Schema.Type.STRING);
      Schema.Field keyField =
          new Schema.Field(KeyValueObjectInspector.KEY_FIELD_NAME, keySchema, null, null);
      Schema valueSchema =
          getAvroSchema(
              moi.getMapValueObjectInspector(),
              bigqueryField.getSubFields().get(KeyValueObjectInspector.VALUE_FIELD_NAME));
      Schema.Field valueField =
          new Schema.Field(KeyValueObjectInspector.VALUE_FIELD_NAME, valueSchema, null, null);
      Schema entrySchema =
          Schema.createRecord(
              "map_" + UUID.randomUUID().toString().replace("-", ""), null, null, false);
      entrySchema.setFields(Arrays.asList(keyField, valueField));
      return modedAvroSchema(Schema.createArray(entrySchema), nullable);
    }
    if (fieldOi instanceof ByteObjectInspector
        || fieldOi instanceof ShortObjectInspector
        || fieldOi instanceof IntObjectInspector
        || (fieldOi instanceof LongObjectInspector)) {
      return modedAvroSchema(Schema.create(Schema.Type.LONG), nullable);
    }
    if (fieldOi instanceof TimestampObjectInspector) {
      Schema schema = Schema.create(Schema.Type.LONG);
      if (bigqueryField.getType().getStandardType().equals(StandardSQLTypeName.TIMESTAMP)) {
        AvroUtils.addProp(schema, "logicalType", "timestamp-micros");
      } else if (bigqueryField.getType().getStandardType().equals(StandardSQLTypeName.DATETIME)) {
        AvroUtils.addProp(schema, "logicalType", "local-timestamp-micros");
      } else {
        throw new RuntimeException(
            String.format(
                "Unexpected BigQuery type `%s` for field `%s` with Hive type `%s`",
                bigqueryField.getType().getStandardType(),
                bigqueryField.getName(),
                fieldOi.getTypeName()));
      }
      return modedAvroSchema(schema, nullable);
    }
    if (fieldOi instanceof TimestampLocalTZObjectInspector) {
      Schema schema = Schema.create(Schema.Type.LONG);
      AvroUtils.addProp(schema, "logicalType", "timestamp-micros");
      return modedAvroSchema(schema, nullable);
    }
    if (fieldOi instanceof DateObjectInspector) {
      Schema schema = Schema.create(Schema.Type.INT);
      AvroUtils.addProp(schema, "logicalType", "date");
      return modedAvroSchema(schema, nullable);
    }
    if (fieldOi instanceof FloatObjectInspector || fieldOi instanceof DoubleObjectInspector) {
      return modedAvroSchema(Schema.create(Schema.Type.DOUBLE), nullable);
    }
    if (fieldOi instanceof BooleanObjectInspector) {
      return modedAvroSchema(Schema.create(Schema.Type.BOOLEAN), nullable);
    }
    if (fieldOi instanceof BinaryObjectInspector) {
      return modedAvroSchema(Schema.create(Schema.Type.BYTES), nullable);
    }
    if (fieldOi instanceof HiveCharObjectInspector
        || fieldOi instanceof HiveVarcharObjectInspector
        || fieldOi instanceof StringObjectInspector) {
      return modedAvroSchema(Schema.create(Schema.Type.STRING), nullable);
    }
    if (fieldOi instanceof HiveDecimalObjectInspector) {
      HiveDecimalObjectInspector hdoi = (HiveDecimalObjectInspector) fieldOi;
      Schema schema = Schema.create(Schema.Type.BYTES);
      AvroUtils.addProp(schema, "logicalType", "decimal");
      AvroUtils.addProp(schema, "precision", new Integer(hdoi.precision()));
      AvroUtils.addProp(schema, "scale", new Integer(hdoi.scale()));
      return modedAvroSchema(schema, nullable);
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
   * Creates a temporary Avro file in GCS for the current task to write records to. This file will
   * later be loaded into the destination BigQuery table by the output committer at the end of the
   * job.
   */
  public static DataFileWriter<GenericRecord> createDataFileWriter(
      JobConf jobConf, JobDetails jobDetails, Path filePath) {
    Schema schema = jobDetails.getAvroSchema();
    GenericDatumWriter<GenericRecord> gdw = new GenericDatumWriter<>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(gdw);
    int level =
        jobConf.getInt(AvroOutputFormat.DEFLATE_LEVEL_KEY, CodecFactory.DEFAULT_DEFLATE_LEVEL);
    String codecName = jobConf.get(AvroJob.OUTPUT_CODEC, "deflate");
    CodecFactory factory =
        codecName.equals(DataFileConstants.DEFLATE_CODEC)
            ? CodecFactory.deflateCodec(level)
            : CodecFactory.fromString(codecName);
    dataFileWriter.setCodec(factory);
    try {
      FileSystem fileSystem = filePath.getFileSystem(jobConf);
      FSDataOutputStream fsDataOutputStream = fileSystem.create(filePath);
      dataFileWriter.create(schema, fsDataOutputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return dataFileWriter;
  }
}
