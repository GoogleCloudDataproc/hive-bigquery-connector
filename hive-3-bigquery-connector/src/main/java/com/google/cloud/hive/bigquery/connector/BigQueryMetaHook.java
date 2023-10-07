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
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

public class BigQueryMetaHook extends BigQueryMetaHookBase {

  public BigQueryMetaHook(Configuration conf) {
    super(conf);
  }

  @Override
  protected void setupIngestionTimePartitioning(Table table) throws MetaException {
    // Add the BigQuery pseudo columns to the Hive MetaStore schema.
    assertDoesNotContainColumn(table, HiveBigQueryConfig.PARTITION_TIME_PSEUDO_COLUMN);
    table
        .getSd()
        .addToCols(
            new FieldSchema(
                HiveBigQueryConfig.PARTITION_TIME_PSEUDO_COLUMN,
                "timestamp with local time zone",
                "Ingestion time pseudo column"));
    assertDoesNotContainColumn(table, HiveBigQueryConfig.PARTITION_DATE_PSEUDO_COLUMN);
    table
        .getSd()
        .addToCols(
            new FieldSchema(
                HiveBigQueryConfig.PARTITION_DATE_PSEUDO_COLUMN,
                "date",
                "Ingestion time pseudo column"));
  }

  @Override
  protected void setupStats(Table table) {
    StatsSetupConst.setStatsStateForCreateTable(table.getParameters(), null, StatsSetupConst.FALSE);
  }

  @Override
  protected List<PrimitiveObjectInspector.PrimitiveCategory> getSupportedTypes() {
    List<PrimitiveObjectInspector.PrimitiveCategory> supportedTypes = super.getSupportedTypes();
    supportedTypes.add(PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMPLOCALTZ);
    return supportedTypes;
  }
}
