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
package com.google.cloud.hive.bigquery.connector.output.indirect;

import static com.google.cloud.hive.bigquery.connector.utils.bq.BigQueryValueConverter.convertHiveValueToBigQuery;

import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.utils.avro.AvroSchemaInfo;
import com.google.cloud.hive.bigquery.connector.utils.avro.AvroUtils;
import com.google.cloud.hive.bigquery.connector.utils.hive.KeyValueObjectInspector;
import java.util.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.hive.serde2.objectinspector.*;

public class AvroDeserializer {

  /**
   * Converts the given Hive-serialized object into an Avro record, so it can later be written to
   * GCS and then loaded into BigQuery via the File Load API.
   */
  public static Record buildSingleRecord(
      StructObjectInspector soi, Schema avroSchema, Object object) {
    Record record = new Record(avroSchema);
    List<? extends StructField> allStructFieldRefs = soi.getAllStructFieldRefs();
    List<Object> structFieldsDataAsList = soi.getStructFieldsDataAsList(object);
    for (int fieldIndex = 0; fieldIndex < avroSchema.getFields().size(); fieldIndex++) {
      Object hiveValue = structFieldsDataAsList.get(fieldIndex);
      ObjectInspector fieldObjectInspector =
          allStructFieldRefs.get(fieldIndex).getFieldObjectInspector();
      String fieldName = allStructFieldRefs.get(fieldIndex).getFieldName();
      Schema fieldSchema = avroSchema.getField(fieldName).schema();
      Object avroValue = convertHiveValueToAvroValue(fieldObjectInspector, fieldSchema, hiveValue);
      record.put(fieldIndex, avroValue);
    }
    return record;
  }

  private static Object convertHiveValueToAvroValue(
      ObjectInspector objectInspector, Schema fieldSchema, Object hiveValue) {
    if (hiveValue == null) {
      return null;
    }

    AvroSchemaInfo schemaInfo = AvroUtils.getSchemaInfo(fieldSchema);

    if (objectInspector instanceof ListObjectInspector) { // Array type
      ListObjectInspector loi = (ListObjectInspector) objectInspector;
      ObjectInspector elementObjectInspector = loi.getListElementObjectInspector();
      Iterator<?> iterator = loi.getList(hiveValue).iterator();
      Schema elementSchema = schemaInfo.getActualSchema().getElementType();
      List<Object> array = new ArrayList<>();
      while (iterator.hasNext()) {
        Object elementValue = iterator.next();
        Object converted =
            convertHiveValueToAvroValue(elementObjectInspector, elementSchema, elementValue);
        array.add(converted);
      }
      return array;
    }

    if (objectInspector instanceof StructObjectInspector) { // Record/Struct type
      return buildSingleRecord(
          (StructObjectInspector) objectInspector, schemaInfo.getActualSchema(), hiveValue);
    }

    if (objectInspector instanceof MapObjectInspector) { // Map type
      // Convert the map into a list of key/value Avro records
      MapObjectInspector moi = (MapObjectInspector) objectInspector;
      List<Object> array = new ArrayList<>();
      Map<?, ?> map = moi.getMap(hiveValue);
      Schema valueSchema =
          schemaInfo
              .getActualSchema()
              .getElementType()
              .getField(KeyValueObjectInspector.VALUE_FIELD_NAME)
              .schema();
      Record record = new Record(schemaInfo.getActualSchema().getElementType());
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        Object key = entry.getKey().toString();
        Object convertedValue =
            convertHiveValueToAvroValue(
                moi.getMapValueObjectInspector(), valueSchema, entry.getValue());
        record.put(KeyValueObjectInspector.KEY_FIELD_NAME, key);
        record.put(KeyValueObjectInspector.VALUE_FIELD_NAME, convertedValue);
        array.add(record);
      }
      return array;
    }

    return convertHiveValueToBigQuery(
        objectInspector, hiveValue, HiveBigQueryConfig.WRITE_METHOD_INDIRECT);
  }
}
