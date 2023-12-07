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
package com.google.cloud.hive.bigquery.connector.sparksql;

import com.google.cloud.hive.bigquery.connector.utils.JobUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.SparkStrategy;
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class HiveBigQuerySparkStrategy extends SparkStrategy {

  @Override
  public Seq<SparkPlan> apply(LogicalPlan plan) {
    Seq<SparkPlan> emptySeq =
        JavaConverters.asScalaBufferConverter(new ArrayList<SparkPlan>()).asScala().toSeq();

    // Check if the Spark job file was already created
    SparkSession session = SparkSession.getActiveSession().get();
    Configuration conf = convertSparkConfToHadoopConf(session.conf());
    Path path = new Path(JobUtils.getQueryWorkDir(conf), "spark-job.json");
    try {
      FileSystem fileSystem = path.getFileSystem(conf);
      if (fileSystem.exists(path)) {
        // File already created, so we're done
        return emptySeq;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    List<String> overwriteTables = new ArrayList<>();
    List<String> insertTables = new ArrayList<>();
    parsePlan(plan, insertTables, overwriteTables);
    if (insertTables.size() > 0 || overwriteTables.size() > 0) {
      SparkSQLUtils.writeSparkJobFile(conf, insertTables, overwriteTables);
    }
    return emptySeq;
  }

  public static Configuration convertSparkConfToHadoopConf(RuntimeConfig sparkConf) {
    Configuration hadoopConf = new Configuration();
    scala.collection.immutable.Map<String, String> scalaMap = sparkConf.getAll();
    java.util.Map<String, String> javaMap = JavaConverters.mapAsJavaMapConverter(scalaMap).asJava();
    for (java.util.Map.Entry<String, String> entry : javaMap.entrySet()) {
      hadoopConf.set(entry.getKey(), entry.getValue());
    }
    return hadoopConf;
  }

  /**
   * Parses the Spark execution plan and finds the BigQuery tables that will be written to, either
   * with "INSERT INTO" or "INSERT OVERWRITE".
   */
  private void parsePlan(
      LogicalPlan plan, List<String> insertTables, List<String> overwriteTables) {
    if (plan instanceof InsertIntoHiveTable) {
      InsertIntoHiveTable insertPlan = (InsertIntoHiveTable) plan;
      if (insertPlan.table().properties().get("storage_handler").nonEmpty()
          && insertPlan
              .table()
              .properties()
              .get("storage_handler")
              .get()
              .equals("com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler")) {
        String tableName =
            String.format(
                "%s.%s",
                insertPlan.table().identifier().database().get(),
                insertPlan.table().identifier().table());
        if (insertPlan.overwrite()) {
          overwriteTables.add(tableName);
        } else {
          insertTables.add(tableName);
        }
      }
    }
    for (LogicalPlan child : JavaConverters.seqAsJavaListConverter(plan.children()).asJava()) {
      parsePlan(child, insertTables, overwriteTables);
    }
  }
}
