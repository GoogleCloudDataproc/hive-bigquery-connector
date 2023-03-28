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
import com.google.cloud.hive.bigquery.connector.JobDetails;
import com.google.cloud.hive.bigquery.connector.output.indirect.IndirectUtils;
import com.google.cloud.hive.bigquery.connector.utils.hive.HiveUtils;
import com.google.cloud.hive.bigquery.connector.utils.hive.KeyValueObjectInspector;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import org.apache.avro.Schema;
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
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptID;

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
      return Schema.createUnion(
          Schema.createArray(getAvroSchema(elementOi, bigqueryField)),
          Schema.create(Schema.Type.NULL));
    }
    if (fieldOi instanceof StructObjectInspector) {
      return Schema.createUnion(
          getAvroSchema((StructObjectInspector) fieldOi, bigqueryField.getSubFields()),
          Schema.create(Schema.Type.NULL));
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
      return Schema.createUnion(Schema.createArray(entrySchema), Schema.create(Schema.Type.NULL));
    }
    if (fieldOi instanceof ByteObjectInspector
        || fieldOi instanceof ShortObjectInspector
        || fieldOi instanceof IntObjectInspector
        || (fieldOi instanceof LongObjectInspector)) {
      return Schema.createUnion(Schema.create(Schema.Type.LONG), Schema.create(Schema.Type.NULL));
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
      return Schema.createUnion(schema, Schema.create(Schema.Type.NULL));
    }
    if (fieldOi instanceof TimestampLocalTZObjectInspector) {
      Schema schema = Schema.create(Schema.Type.LONG);
      schema.addProp("logicalType", "timestamp-micros");
      return Schema.createUnion(schema, Schema.create(Schema.Type.NULL));
    }
    if (fieldOi instanceof DateObjectInspector) {
      Schema schema = Schema.create(Schema.Type.INT);
      schema.addProp("logicalType", "date");
      return Schema.createUnion(schema, Schema.create(Schema.Type.NULL));
    }
    if (fieldOi instanceof FloatObjectInspector || fieldOi instanceof DoubleObjectInspector) {
      return Schema.createUnion(Schema.create(Schema.Type.DOUBLE), Schema.create(Schema.Type.NULL));
    }
    if (fieldOi instanceof BooleanObjectInspector) {
      return Schema.createUnion(
          Schema.create(Schema.Type.BOOLEAN), Schema.create(Schema.Type.NULL));
    }
    if (fieldOi instanceof BinaryObjectInspector) {
      return Schema.createUnion(Schema.create(Schema.Type.BYTES), Schema.create(Schema.Type.NULL));
    }
    if (fieldOi instanceof HiveCharObjectInspector
        || fieldOi instanceof HiveVarcharObjectInspector
        || fieldOi instanceof StringObjectInspector) {
      return Schema.createUnion(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL));
    }
    if (fieldOi instanceof HiveDecimalObjectInspector) {
      HiveDecimalObjectInspector hdoi = (HiveDecimalObjectInspector) fieldOi;
      Schema schema = Schema.create(Schema.Type.BYTES);
      schema.addProp("logicalType", "decimal");
      schema.addProp("precision", hdoi.precision());
      schema.addProp("scale", hdoi.scale());
      return Schema.createUnion(schema, Schema.create(Schema.Type.NULL));
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
      JobConf jobConf, JobDetails jobDetails) {
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
    dataFileWriter.setMeta(AvroSerDe.WRITER_TIME_ZONE, TimeZone.getDefault().toZoneId().toString());
    TaskAttemptID taskAttemptID = HiveUtils.taskAttemptIDWrapper(jobConf);
    Path filePath =
        IndirectUtils.getTaskAvroTempFile(
            jobConf,
            jobDetails.getHmsDbTableName(),
            jobDetails.getTableId(),
            jobDetails.getGcsTempPath(),
            taskAttemptID);
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
