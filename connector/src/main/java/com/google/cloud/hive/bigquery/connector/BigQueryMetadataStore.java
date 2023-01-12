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

import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientModule;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConnectorModule;
import com.google.cloud.hive.bigquery.connector.input.BigQueryFilters;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.*;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.thrift.TException;
import repackaged.by.hivebqconnector.com.google.common.collect.Streams;

public class BigQueryMetadataStore extends ObjectStore {

  PartitionExpressionProxy partitionExpressionProxy;

  protected Table getBigQueryLinkedTable(String catName, String dbName, String tableName)
      throws MetaException {
    HiveMetaStoreClient client = new HiveMetaStoreClient(getConf());
    Table table;
    try {
      table = client.getTable(catName, dbName, tableName);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
    String storageHandler = table.getParameters().get("storage_handler");
    if (storageHandler == null
        || !storageHandler.equals(
            "com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler")) {
      return null;
    }
    return table;
  }

  public static ExprNodeDesc convertFilter(ExprNodeDesc filterExpr) {
    // Check if it's a function
    if (filterExpr instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc function = ((ExprNodeGenericFuncDesc) filterExpr);
      // Translate the children parameters
      List<ExprNodeDesc> translatedChildren = new ArrayList<>();
      for (ExprNodeDesc child : filterExpr.getChildren()) {
        translatedChildren.add(convertFilter(child));
      }
      function.setChildren(translatedChildren);
      return filterExpr;
    }
    // Check if it's a column
    if (filterExpr instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc columnDesc = ((ExprNodeColumnDesc) filterExpr);
      columnDesc.setColumn("partition_id");
      return columnDesc;
    }
    // Check if it's a constant value
    if (filterExpr instanceof ExprNodeConstantDesc) {
      ExprNodeConstantDesc constantDesc = (ExprNodeConstantDesc) filterExpr;
      // TODO: Do something more robust based on the column type (date, timestamp, etc.)
      constantDesc.setValue(((String) constantDesc.getValue()).replace("-", ""));
      return constantDesc;
    }
    throw new RuntimeException("Unexpected filter type: " + filterExpr);
  }

  protected List<String> fetchPartitionIds(Table table, ExprNodeGenericFuncDesc filter, short max) {
    // Fetch partition ids from BigQuery
    Injector injector =
        Guice.createInjector(
            new BigQueryClientModule(),
            new HiveBigQueryConnectorModule(getConf(), table.getParameters()));
    BigQueryClient bqClient = injector.getInstance(BigQueryClient.class);
    HiveBigQueryConfig config = injector.getInstance(HiveBigQueryConfig.class);
    TableId tableId = config.getTableId();
    String convertedFilter = null;
    if (filter != null) {
      convertedFilter = convertFilter(filter).getExprString();
    }
    String query =
        String.format(
            "SELECT partition_id FROM `%s.%s.INFORMATION_SCHEMA.PARTITIONS` WHERE table_name ="
                + " '%s' %s %s",
            tableId.getProject(),
            tableId.getDataset(),
            tableId.getTable(),
            convertedFilter != null ? "AND " + convertedFilter : "",
            max > 0 ? "LIMIT " + max : "");
    TableResult bqPartitions = bqClient.query(query);
    // Convert the BigQuery partition ids to the format expected by Hive
    List<String> partitionNames = new ArrayList<>();
    StandardTableDefinition tableDef = bqClient.getTable(config.getTableId()).getDefinition();
    TimePartitioning timePartitioning = tableDef.getTimePartitioning();
    if (timePartitioning != null && timePartitioning.getType().equals(TimePartitioning.Type.DAY)) {
      List<FieldValueList> rows =
          Streams.stream(bqPartitions.iterateAll()).collect(Collectors.toList());
      for (FieldValueList value : rows) {
        // In BQ, DAY partition ids are formatted as YYYYMMDD.
        SimpleDateFormat bqFormat = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat hiveFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date date;
        try {
          date = bqFormat.parse(value.get(0).getStringValue());
        } catch (ParseException e) {
          throw new RuntimeException(e);
        }
        partitionNames.add(hiveFormat.format(date));
      }
    }
    return partitionNames;
  }

  protected List<Partition> fetchPartitionsFromBigQuery(
      Table table, String catName, String dbName, String tableName, ExprNodeGenericFuncDesc filter)
      throws MetaException {
    String partitionColumnName = table.getPartitionKeys().get(0).getName();
    List<String> values = fetchPartitionIds(table, filter, (short) -1);
    List<Partition> result = new ArrayList<>();
    for (String value : values) {
      Partition partition = new Partition();
      StorageDescriptor sd = new StorageDescriptor();
      sd.setSerdeInfo(table.getSd().getSerdeInfo());
      Map<String, String> map = new HashMap<>();
      map.put(partitionColumnName, value);
      Path location = new Path(table.getSd().getLocation(), Warehouse.makePartPath(map));
      sd.setLocation(location.toString());
      partition.setSd(sd);
      partition.setParameters(new HashMap<>());
      partition.setValues(Collections.singletonList(value));
      partition.setCatName(catName);
      partition.setDbName(dbName);
      partition.setTableName(tableName);
      result.add(partition);
    }
    return result;
  }

  /** Returns: Whether the list has any partitions for which the expression may or may not match. */
  @Override
  public boolean getPartitionsByExpr(
      String catName,
      String dbName,
      String tableName,
      byte[] expr,
      String defaultPartitionName,
      short maxParts,
      List<Partition> result)
      throws TException {
    Table table = getBigQueryLinkedTable(catName, dbName, tableName);
    if (table == null) {
      // This is not a Hive table linked to a BigQuery table
      return super.getPartitionsByExpr(
          catName, dbName, tableName, expr, defaultPartitionName, maxParts, result);
    }
    ExprNodeGenericFuncDesc filterExpr = SerializationUtilities.deserializeExpressionFromKryo(expr);
    ExprNodeGenericFuncDesc translatedFilterExpr =
        (ExprNodeGenericFuncDesc) BigQueryFilters.translateFilters(filterExpr);
    result.addAll(
        fetchPartitionsFromBigQuery(table, catName, dbName, tableName, translatedFilterExpr));
    return true; // TODO: Figure out what to return
  }

  @Override
  public List<Partition> getPartitions(
      String catName, String dbName, String tableName, int maxParts)
      throws MetaException, NoSuchObjectException {
    Table table = getBigQueryLinkedTable(catName, dbName, tableName);
    if (table == null) {
      // This is not a Hive table linked to a BigQuery table
      return super.getPartitions(catName, dbName, tableName, maxParts);
    }
    return fetchPartitionsFromBigQuery(table, catName, dbName, tableName, null);
  }

  /** Called by "SHOW PARTITIONS". */
  @Override
  public List<String> listPartitionNames(String catName, String dbName, String tableName, short max)
      throws MetaException {
    Table table = getBigQueryLinkedTable(catName, dbName, tableName);
    if (table == null) {
      // This is not a Hive table linked to a BigQuery table
      return super.listPartitionNames(catName, dbName, tableName, max);
    }
    String partitionColumnName = table.getPartitionKeys().get(0).getName();
    List<String> values = fetchPartitionIds(table, null, max);
    List<String> result = new ArrayList<>();
    for (String value : values) {
      result.add(String.format("%s=%s", partitionColumnName, value));
    }
    return result;
  }
}
