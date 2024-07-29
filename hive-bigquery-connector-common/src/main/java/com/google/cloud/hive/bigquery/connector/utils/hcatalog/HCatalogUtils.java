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
package com.google.cloud.hive.bigquery.connector.utils.hcatalog;

import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.mapreduce.HCatTableInfo;
import org.apache.hive.hcatalog.mapreduce.InputJobInfo;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;

public class HCatalogUtils {
  public static final String HCAT_OUTPUT_ID_HASH = "mapreduce.lib.hcatoutput.id";
  public static final String HCAT_OUTPUT_SCHEMA = "hcat.output.schema";

  public static boolean isHCatalogInputJob(Configuration conf) {
    return conf.get(HCatConstants.HCAT_KEY_JOB_INFO, "").trim().length() > 0;
  }

  public static boolean isHCatalogOutputJob(Configuration conf) {
    return conf.get(HCatConstants.HCAT_KEY_OUTPUT_INFO, "").trim().length() > 0;
  }

  /** Deserializes the HCatalog job information object from the Hadoop conf. */
  public static InputJobInfo getHCatalogInputJobInfo(Configuration conf) {
    InputJobInfo inputJobInfo;
    try {
      inputJobInfo = (InputJobInfo) HCatUtil.deserialize(conf.get(HCatConstants.HCAT_KEY_JOB_INFO));
    } catch (IOException e) {
      throw new RuntimeException(
          String.format(
              "Failed to deserialize HCatalog input job info configuration property (%s)",
              HCatConstants.HCAT_KEY_JOB_INFO));
    }
    return inputJobInfo;
  }

  /** Deserializes the HCatalog output job information object from the Hadoop conf. */
  public static OutputJobInfo getHCatalogOutputJobInfo(Configuration conf) {
    OutputJobInfo outputJobInfo;
    try {
      outputJobInfo =
          (OutputJobInfo) HCatUtil.deserialize(conf.get(HCatConstants.HCAT_KEY_OUTPUT_INFO));
    } catch (IOException e) {
      throw new RuntimeException(
          String.format(
              "Failed to deserialize HCatalog output job info configuration property (%s)",
              HCatConstants.HCAT_KEY_OUTPUT_INFO));
    }
    return outputJobInfo;
  }

  /**
   * If we're using HCatalog, set some needed Hadoop conf properties. Those properties would
   * otherwise already be set automatically by Hive outside of using HCatalog.
   */
  public static void updateHadoopConfForHCatalog(Configuration conf, HCatTableInfo tableInfo) {
    Properties properties = createProperties(tableInfo);
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      conf.set(entry.getKey().toString(), entry.getValue().toString());
    }
  }

  public static void updateTablePropertiesForHCatalog(
      Properties properties, HCatTableInfo tableInfo) {
    properties.putAll(createProperties(tableInfo));
  }

  private static Properties createProperties(HCatTableInfo tableInfo) {
    Properties properties = new Properties();
    properties.put(hive_metastoreConstants.META_TABLE_LOCATION, tableInfo.getTableLocation());
    properties.put(HiveBigQueryConfig.HIVE_COLUMN_NAME_DELIMITER, String.valueOf(SerDeUtils.COMMA));
    properties.put(
        serdeConstants.LIST_COLUMNS,
        String.join(String.valueOf(SerDeUtils.COMMA), tableInfo.getDataColumns().getFieldNames()));
    List<String> columnTypes = new ArrayList<>();
    for (int i = 0; i < tableInfo.getDataColumns().size(); i++) {
      HCatFieldSchema schema = tableInfo.getDataColumns().get(i);
      columnTypes.add(schema.getTypeString());
    }
    properties.put(serdeConstants.LIST_COLUMN_TYPES, String.join(":", columnTypes));
    return properties;
  }
}
