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
package com.google.cloud.hive.bigquery.connector.utils.bq;

import com.google.cloud.bigquery.*;
import com.google.cloud.hive.bigquery.connector.HiveCompat;
import com.google.cloud.hive.bigquery.connector.utils.hcatalog.HCatalogUtils;
import com.google.cloud.hive.bigquery.connector.utils.hive.HiveUtils;
import com.google.cloud.hive.bigquery.connector.utils.hive.KeyValueObjectInspector;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

/** Converts Hive Schema to BigQuery schema. */
public class BigQuerySchemaConverter {

  public static Schema toBigQuerySchema(Configuration conf, TableDesc tableDesc) {
    if (HCatalogUtils.isHCatalogOutputJob(conf)) {
      HCatSchema hcatSchema = HCatalogUtils.getHCatalogOutputJobInfo(conf).getOutputSchema();
      return toBigQuerySchema(hcatSchema);
    }
    // Fetch the Hive schema
    Hive hive;
    HiveConf hiveConf =
        conf instanceof HiveConf ? (HiveConf) conf : new HiveConf(conf, HiveUtils.class);
    try {
      hive = Hive.get(hiveConf);
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
    String[] dbAndTableNames = tableDesc.getTableName().split("\\.");
    if (dbAndTableNames.length != 2
        || dbAndTableNames[0].isEmpty()
        || dbAndTableNames[1].isEmpty()) {
      throw new IllegalArgumentException(
          "Invalid table name format. Expected format 'dbName.tblName'. Received: "
              + tableDesc.getTableName());
    }
    Table table;
    try {
      table = hive.getTable(dbAndTableNames[0], dbAndTableNames[1]);
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
    // Convert the Hive schema to BigQuery schema
    return toBigQuerySchema(table.getSd());
  }

  /** Converts the provided HCatalog schema to the corresponding BigQuery schema. */
  public static Schema toBigQuerySchema(HCatSchema hcatSchema) {
    List<Field> bigQueryFields = new ArrayList<>();
    for (HCatFieldSchema hiveField : hcatSchema.getFields()) {
      TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(hiveField.getTypeString());
      bigQueryFields.add(buildBigQueryField(hiveField.getName(), typeInfo, hiveField.getComment()));
    }
    return Schema.of(bigQueryFields);
  }

  /** Converts the provided Hive schema to the corresponding BigQuery schema. */
  public static Schema toBigQuerySchema(StorageDescriptor sd) {
    List<Field> bigQueryFields = new ArrayList<>();
    for (FieldSchema hiveField : sd.getCols()) {
      TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(hiveField.getType());
      bigQueryFields.add(buildBigQueryField(hiveField.getName(), typeInfo, hiveField.getComment()));
    }
    return Schema.of(bigQueryFields);
  }

  private static Field buildBigQueryField(String fieldName, TypeInfo typeInfo, String comment) {
    Field.Builder bigQueryFieldBuilder;

    Field.Mode mode = null;
    if (typeInfo instanceof ListTypeInfo) {
      typeInfo = ((ListTypeInfo) typeInfo).getListElementTypeInfo();
      mode = Field.Mode.REPEATED;
    }

    if (typeInfo instanceof MapTypeInfo) {
      // Convert the Hive Map type to a list (i.e. REPEATED) of key/values STRUCTs
      ArrayList<TypeInfo> subTypeInfos =
          new ArrayList<>(
              Arrays.asList(
                  ((MapTypeInfo) typeInfo).getMapKeyTypeInfo(),
                  ((MapTypeInfo) typeInfo).getMapValueTypeInfo()));
      ArrayList<String> subFieldNames =
          new ArrayList<>(
              Arrays.asList(
                  KeyValueObjectInspector.KEY_FIELD_NAME,
                  KeyValueObjectInspector.VALUE_FIELD_NAME));
      typeInfo = new StructTypeInfo();
      ((StructTypeInfo) typeInfo).setAllStructFieldNames(subFieldNames);
      ((StructTypeInfo) typeInfo).setAllStructFieldTypeInfos(subTypeInfos);
      mode = Field.Mode.REPEATED;
    }

    if (typeInfo instanceof StructTypeInfo) {
      List<TypeInfo> subFieldTypeInfos = ((StructTypeInfo) typeInfo).getAllStructFieldTypeInfos();
      List<String> subFieldNames = ((StructTypeInfo) typeInfo).getAllStructFieldNames();
      List<Field> bigQuerySubFields = new ArrayList<>();
      for (int i = 0; i < subFieldNames.size(); i++) {
        Field bigQuerySubField =
            buildBigQueryField(subFieldNames.get(i), subFieldTypeInfos.get(i), null);
        bigQuerySubFields.add(bigQuerySubField);
      }
      bigQueryFieldBuilder =
          Field.newBuilder(fieldName, LegacySQLTypeName.RECORD, FieldList.of(bigQuerySubFields));
    } else if (typeInfo instanceof DecimalTypeInfo) {
      int precision = ((DecimalTypeInfo) typeInfo).getPrecision();
      int scale = ((DecimalTypeInfo) typeInfo).getScale();
      // HiveDecimal has precision/scale [38, 38]
      // BigQuery
      // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
      boolean useBig = scale > 9 || precision > scale + 29;
      StandardSQLTypeName fieldType =
          useBig ? StandardSQLTypeName.BIGNUMERIC : StandardSQLTypeName.NUMERIC;
      bigQueryFieldBuilder = Field.newBuilder(fieldName, fieldType);
      // Do not set precision/scale if with Bigquery default precision 38 scale 9
      if (!(precision == 38 && scale == 9)) {
        bigQueryFieldBuilder.setPrecision((long) precision).setScale((long) scale);
      }
    } else {
      StandardSQLTypeName fieldType = toBigQueryFieldType(typeInfo);
      bigQueryFieldBuilder = Field.newBuilder(fieldName, fieldType);
    }

    if (mode != null) {
      bigQueryFieldBuilder.setMode(mode);
    }
    bigQueryFieldBuilder.setDescription(comment);
    return bigQueryFieldBuilder.build();
  }

  private static StandardSQLTypeName toBigQueryFieldType(TypeInfo typeInfo) {
    if (typeInfo instanceof PrimitiveTypeInfo) {
      PrimitiveObjectInspector.PrimitiveCategory category =
          ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
      return Preconditions.checkNotNull(
          HiveCompat.getInstance().getHiveToBqTypeMappings().get(category),
          new IllegalStateException("Unexpected type: " + category.name()));
    } else {
      throw new IllegalStateException("Unexpected type: " + typeInfo.getCategory().name());
    }
  }
}
