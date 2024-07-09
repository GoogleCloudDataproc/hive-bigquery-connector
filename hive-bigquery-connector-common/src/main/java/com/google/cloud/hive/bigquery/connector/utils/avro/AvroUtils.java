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

import com.google.cloud.bigquery.FieldList;
import com.google.cloud.hive.bigquery.connector.HiveCompat;
import com.google.cloud.hive.bigquery.connector.JobDetails;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.mapred.JobConf;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

public class AvroUtils {

  /**
   * Hive vendors Avro libraries that have different methods depending on the Hive version used.
   * Here we dynamically figure out which Avro methods are available so we can use the proper ones
   * at runtime.
   */
  private static Method getObjectPropMethod;

  private static Method getJsonPropMethod;
  private static Method asIntMethod;
  private static Method getIntValueMethod;
  private static Method addPropJsonNodeMethod;
  private static Method addPropObjectMethod;

  static {
    // Initialize methods for getPropAsInt
    try {
      getObjectPropMethod = Schema.class.getMethod("getObjectProp", String.class);
    } catch (NoSuchMethodException e) {
      getObjectPropMethod = null;
    }

    try {
      getJsonPropMethod = Schema.class.getMethod("getJsonProp", String.class);
      // Assuming getJsonProp returns an Object which has asInt or getIntValue methods
      Class<?> returnType = getJsonPropMethod.getReturnType();
      try {
        asIntMethod = returnType.getMethod("asInt");
      } catch (NoSuchMethodException e) {
        asIntMethod = null;
      }
      try {
        getIntValueMethod = returnType.getMethod("getIntValue");
      } catch (NoSuchMethodException e) {
        getIntValueMethod = null;
      }
    } catch (NoSuchMethodException e) {
      getJsonPropMethod = null;
    }

    // Initialize methods for addProp
    try {
      addPropJsonNodeMethod = Schema.class.getMethod("addProp", String.class, JsonNode.class);
    } catch (NoSuchMethodException e) {
      addPropJsonNodeMethod = null;
    }

    try {
      addPropObjectMethod = Schema.class.getMethod("addProp", String.class, Object.class);
    } catch (NoSuchMethodException e) {
      addPropObjectMethod = null;
    }
  }

  public static void addProp(Schema schema, String propName, String propValue) {
    schema.addProp(propName, propValue);
  }

  public static void addProp(Schema schema, String propName, Object propValue) {
    try {
      if (addPropJsonNodeMethod != null) {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNodeValue = objectMapper.convertValue(propValue, JsonNode.class);
        addPropJsonNodeMethod.invoke(schema, propName, jsonNodeValue);
      } else if (addPropObjectMethod != null) {
        addPropObjectMethod.invoke(schema, propName, propValue);
      } else {
        throw new RuntimeException("Unsupported version of Avro");
      }
    } catch (Exception e) {
      throw new RuntimeException("Error accessing Avro addProp method", e);
    }
  }

  public static int getPropAsInt(Schema schema, String propName) {
    try {
      if (getObjectPropMethod != null) {
        return (int) getObjectPropMethod.invoke(schema, propName);
      } else if (getJsonPropMethod != null) {
        Object jsonProp = getJsonPropMethod.invoke(schema, propName);
        if (asIntMethod != null) {
          return (int) asIntMethod.invoke(jsonProp);
        } else if (getIntValueMethod != null) {
          return (int) getIntValueMethod.invoke(jsonProp);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Error accessing Avro integer property method", e);
    }
    throw new RuntimeException("Unsupported version of Avro");
  }

  public static Schema getAvroSchema(StructObjectInspector soi, FieldList bigqueryFields) {
    List<Schema.Field> avroFields = new ArrayList<>();
    List<? extends StructField> allStructFieldRefs = soi.getAllStructFieldRefs();
    for (int i = 0; i < allStructFieldRefs.size(); i++) {
      StructField structField = allStructFieldRefs.get(i);
      Schema fieldSchema =
          HiveCompat.getInstance()
              .getAvroSchema(structField.getFieldObjectInspector(), bigqueryFields.get(i));
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
  public static Schema nullableAvroSchema(Schema fieldSchema, boolean nullable) {
    return nullable
        ? Schema.createUnion(Arrays.asList(fieldSchema, Schema.create(Schema.Type.NULL)))
        : fieldSchema;
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
        jobConf.getInt(AvroOutputFormat.DEFLATE_LEVEL_KEY, 6); // 6 = XZCodec.DEFAULT_COMPRESSION
    String codecName = jobConf.get(AvroJob.OUTPUT_CODEC, "deflate");
    CodecFactory factory =
        codecName.equals(DataFileConstants.DEFLATE_CODEC)
            ? CodecFactory.deflateCodec(level)
            : CodecFactory.fromString(codecName);
    dataFileWriter.setCodec(factory);
    try {
      FileSystem fileSystem = filePath.getFileSystem(jobConf);
      FSDataOutputStream fsDataOutputStream = fileSystem.create(filePath, true);
      dataFileWriter.create(schema, fsDataOutputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return dataFileWriter;
  }
}
