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

import com.google.cloud.hive.bigquery.connector.utils.hive.KeyValueObjectInspector;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StringInternUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeException;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
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

  /**
   * Makes some modifications to the provided Avro schema to be compatible with the way we store
   * Hive data in BigQuery.
   */
  public static Schema adaptSchemaForBigQuery(Schema originalSchema) {
    AvroSchemaInfo schemaInfo = AvroUtils.getSchemaInfo(originalSchema);
    Schema schema = schemaInfo.getActualSchema();

    if (schema.getType() == Schema.Type.ARRAY) {
      Schema modifiedElementType = adaptSchemaForBigQuery(schema.getElementType());
      schema = Schema.createArray(modifiedElementType);
    } else if (schema.getType() == Schema.Type.RECORD) {
      List<Schema.Field> originalFields = schema.getFields();
      List<Schema.Field> modifiedFields = new ArrayList<>();
      originalFields.forEach(
          originalField -> {
            Schema modifiedFieldSchema = adaptSchemaForBigQuery(originalField.schema());
            modifiedFields.add(
                new Schema.Field(
                    originalField.name(),
                    modifiedFieldSchema,
                    originalField.doc(),
                    originalField.defaultValue()));
          });
      schema =
          Schema.createRecord(
              "record_" + UUID.randomUUID().toString().replace("-", ""), null, null, false);
      schema.setFields(modifiedFields);
    } else if (schema.getType() == Schema.Type.MAP) {
      // Convert the Map type into a list of key/value records
      Schema keySchema = Schema.create(Schema.Type.STRING);
      Schema.Field keyField =
          new Schema.Field(KeyValueObjectInspector.KEY_FIELD_NAME, keySchema, null, null);
      Schema valueSchema = adaptSchemaForBigQuery(schema.getValueType());
      Schema.Field valueField =
          new Schema.Field(KeyValueObjectInspector.VALUE_FIELD_NAME, valueSchema, null, null);
      Schema entrySchema =
          Schema.createRecord(
              "map_" + UUID.randomUUID().toString().replace("-", ""), null, null, false);
      entrySchema.setFields(Arrays.asList(keyField, valueField));
      schema = Schema.createArray(entrySchema);
    }

    if (schemaInfo.isNullable()) {
      schema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), schema));
    }

    return schema;
  }

  /**
   * Returns true if the table properties contain an explicit Avro schema, either with
   * `avro.schema.literal` or `avro.schema.url`.
   */
  public static boolean hasExplicitAvroSchema(Properties tableProperties) {
    return tableProperties.getProperty(
                AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName())
            != null
        || tableProperties.getProperty(AvroSerdeUtils.AvroTableProperties.SCHEMA_URL.getPropName())
            != null;
  }

  /**
   * Extracts the Avro schema from the `columns` and `column.types` table properties, if present.
   * Otherwise, returns null.
   */
  public static Schema extractSchemaFromColumnProperties(Properties tableProperties) {
    String columnNameProperty = tableProperties.getProperty(serdeConstants.LIST_COLUMNS);
    String columnTypeProperty = tableProperties.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    String columnCommentProperty = tableProperties.getProperty("columns.comments", "");
    String columnNameDelimiter =
        tableProperties.containsKey(serdeConstants.COLUMN_NAME_DELIMITER)
            ? tableProperties.getProperty(serdeConstants.COLUMN_NAME_DELIMITER)
            : String.valueOf(',');
    if (columnNameProperty != null
        && !columnNameProperty.isEmpty()
        && columnTypeProperty != null
        && !columnTypeProperty.isEmpty()) {
      List<String> columnNames =
          StringInternUtils.internStringsInList(
              Arrays.asList(columnNameProperty.split(columnNameDelimiter)));
      ArrayList<TypeInfo> columnTypes =
          TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
      return AvroSerDe.getSchemaFromCols(
          tableProperties, columnNames, columnTypes, columnCommentProperty);
    }
    return null;
  }

  /** Extract the Avro schema from the given table properties. */
  public static Schema extractAvroSchema(Configuration conf, Properties tableProperties) {
    Schema schema = null;
    if (!hasExplicitAvroSchema(tableProperties)) {
      schema = extractSchemaFromColumnProperties(tableProperties);
    }
    if (schema == null) {
      try {
        schema = AvroSerdeUtils.determineSchemaOrThrowException(conf, tableProperties);
      } catch (IOException | AvroSerdeException e) {
        throw new RuntimeException(e);
      }
    }
    return schema;
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
