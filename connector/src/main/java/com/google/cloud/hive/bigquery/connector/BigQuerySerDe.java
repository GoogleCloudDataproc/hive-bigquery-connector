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
package com.google.cloud.hive.bigquery.connector;

import com.google.cloud.hive.bigquery.connector.output.BigQueryOutputFormat;
import java.util.*;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;

/**
 * Simple SerDe class that just wraps the already-serialized object into a ObjectWritable. The
 * actual (de)serialization operations are delegated to the `AvroSerializer`, `AvroDeserializer`,
 * `ArrowSerializer`, and `ProtoDeserializer` classes.
 */
public class BigQuerySerDe extends AbstractSerDe {

  private StructObjectInspector rowObjectInspector;

  public static StructObjectInspector getRowObjectInspector(Configuration conf) {
    String columnNameProperty = conf.get(serdeConstants.LIST_COLUMNS);
    String columnTypeProperty = conf.get(serdeConstants.LIST_COLUMN_TYPES);
    String columnNameDelimiter =
        conf.get(serdeConstants.COLUMN_NAME_DELIMITER, String.valueOf(SerDeUtils.COMMA));
    return getRowObjectInspector(columnNameProperty, columnTypeProperty, columnNameDelimiter, null);
  }

  public static StructObjectInspector getRowObjectInspector(
      Map<Object, Object> tableProperties, BigQueryOutputFormat.Partition partition) {
    String columnNameProperty = (String) tableProperties.get(serdeConstants.LIST_COLUMNS);
    String columnTypeProperty = (String) tableProperties.get(serdeConstants.LIST_COLUMN_TYPES);
    String columnNameDelimiter =
        String.valueOf(
            tableProperties.getOrDefault(serdeConstants.COLUMN_NAME_DELIMITER, SerDeUtils.COMMA));
    return getRowObjectInspector(
        columnNameProperty, columnTypeProperty, columnNameDelimiter, partition);
  }

  public static StructObjectInspector getRowObjectInspector(
      String columnNameProperty,
      String columnTypeProperty,
      String columnNameDelimiter,
      BigQueryOutputFormat.Partition partition) {
    List<String> columnNames = new ArrayList<>();
    List<TypeInfo> columnTypes = new ArrayList<>();
    if (columnNameProperty.length() > 0) {
      columnNames.addAll(Arrays.asList(columnNameProperty.split(columnNameDelimiter)));
      columnTypes.addAll(TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty));
    }
    if (partition != null) {
      columnNames.add(partition.getName());
      columnTypes.add(partition.getType());
    }
    StructTypeInfo rowTypeInfo =
        (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
    return (StructObjectInspector)
        TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(rowTypeInfo);
  }

  @Override
  public void initialize(@Nullable Configuration configuration, Properties tableProperties)
      throws SerDeException {
    this.rowObjectInspector = getRowObjectInspector(tableProperties, null);
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
    return new ObjectWritable(o);
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return rowObjectInspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return ObjectWritable.class;
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    return ((ObjectWritable) writable).get();
  }
}
