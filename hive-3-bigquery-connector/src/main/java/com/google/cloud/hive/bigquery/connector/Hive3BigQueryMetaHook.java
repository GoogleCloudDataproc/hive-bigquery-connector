/*
 * Copyright 2023 Google Inc. All Rights Reserved.
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

import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.utils.hive.HiveUtils;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.DefaultHiveMetaHook;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

public class Hive3BigQueryMetaHook extends DefaultHiveMetaHook implements MetahookExtension {

  BigQueryMetaHook metahook;

  public Hive3BigQueryMetaHook(Configuration conf) {
    this.metahook = new BigQueryMetaHook(conf, this);
  }

  @Override
  public void setupIngestionTimePartitioning(Table table) throws MetaException {
    // Add the BigQuery pseudo columns to the Hive MetaStore schema.
    BigQueryMetaHook.assertDoesNotContainColumn(
        table, HiveBigQueryConfig.PARTITION_TIME_PSEUDO_COLUMN);
    table
        .getSd()
        .addToCols(
            new FieldSchema(
                HiveBigQueryConfig.PARTITION_TIME_PSEUDO_COLUMN,
                "timestamp with local time zone",
                "Ingestion time pseudo column"));
    BigQueryMetaHook.assertDoesNotContainColumn(
        table, HiveBigQueryConfig.PARTITION_DATE_PSEUDO_COLUMN);
    table
        .getSd()
        .addToCols(
            new FieldSchema(
                HiveBigQueryConfig.PARTITION_DATE_PSEUDO_COLUMN,
                "date",
                "Ingestion time pseudo column"));
  }

  @Override
  public void setupStats(Table table) {
    StatsSetupConst.setStatsStateForCreateTable(table.getParameters(), null, StatsSetupConst.FALSE);
  }

  @Override
  public List<PrimitiveObjectInspector.PrimitiveCategory> getSupportedTypes() {
    List<PrimitiveObjectInspector.PrimitiveCategory> supportedTypes =
        new ArrayList<>(BigQueryMetaHook.basicTypes);
    supportedTypes.add(PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMPLOCALTZ);
    return supportedTypes;
  }

  @Override
  public void commitInsertTable(Table table, boolean overwrite) throws MetaException {
    metahook.commitInsertTable(HiveUtils.getDbTableName(table));
  }

  @Override
  public void preInsertTable(Table table, boolean overwrite) throws MetaException {
    metahook.preInsertTable(HiveUtils.getDbTableName(table), overwrite);
  }

  @Override
  public void rollbackInsertTable(Table table, boolean overwrite) throws MetaException {
    metahook.rollbackInsertTable(table, overwrite);
  }

  @Override
  public void preCreateTable(Table table) throws MetaException {
    metahook.preCreateTable(table);
  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
    metahook.rollbackCreateTable(table);
  }

  @Override
  public void commitCreateTable(Table table) throws MetaException {
    metahook.commitCreateTable(table);
  }

  @Override
  public void preDropTable(Table table) throws MetaException {
    metahook.preDropTable(table);
  }

  @Override
  public void rollbackDropTable(Table table) throws MetaException {
    metahook.rollbackDropTable(table);
  }

  @Override
  public void commitDropTable(Table table, boolean deleteData) throws MetaException {
    metahook.commitDropTable(table, deleteData);
  }
}
