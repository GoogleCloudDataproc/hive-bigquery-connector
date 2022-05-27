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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.TimeZone;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeException;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.mapred.JobConf;

public class AvroUtils {

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

  /** Parse an Avro schema from the given JSON-formatted string. */
  public static Schema parseSchema(String jsonAvroSchema) {
    Properties properties = new Properties();
    properties.setProperty(
        AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName(), jsonAvroSchema);
    try {
      return AvroSerdeUtils.determineSchemaOrThrowException(new Configuration(), properties);
    } catch (AvroSerdeException | IOException e) {
      throw new RuntimeException(e);
    }
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
