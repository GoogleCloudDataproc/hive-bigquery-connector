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
package com.google.cloud.hive.bigquery.connector.utils.hive;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * Helper class that looks up details about a task ID and Tez Vertex ID. This is useful to create
 * temporary files uniquely named after the individual tasks.
 */
public class HiveUtils {

  public static boolean isExternalTable(Table table) {
    if (table == null) {
      return false;
    }
    Map<String, String> params = table.getParameters();
    if (params == null) {
      return false;
    }
    return "TRUE".equalsIgnoreCase(params.get("EXTERNAL"));
  }

  public static boolean isSparkJob(Configuration conf) {
    return conf.get("spark.app.id", "").length() != 0;
  }

  /** Returns the ID of the Hive query as set by Hive in the configuration. */
  public static String getQueryId(Configuration conf) {
    String hiveQueryId = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEQUERYID, "");
    if (!hiveQueryId.equals("")) {
      return "hive-query-id-" + hiveQueryId;
    }
    String sparkAppId = conf.get("spark.app.id", "");
    if (!sparkAppId.equals("")) {
      return "spark-app-id-" + sparkAppId;
    }
    return "no-query-id";
  }

  public static String getDbTableName(org.apache.hadoop.hive.metastore.api.Table table) {
    return table.getDbName() + "." + table.getTableName();
  }

  public static boolean enableCommitterInTez(Configuration conf) {
    String tezCommitter = conf.get("hive.tez.mapreduce.output.committer.class");
    return (tezCommitter != null
        && tezCommitter.equals("org.apache.tez.mapreduce.committer.MROutputCommitter"));
  }
}
