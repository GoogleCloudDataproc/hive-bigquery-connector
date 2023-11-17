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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskAttemptID;

/**
 * Helper class that looks up details about a task ID and Tez Vertex ID. This is useful to create
 * temporary files uniquely named after the individual tasks.
 */
public class HiveUtils {

  /**
   * Same as `MetaStoreUtils.isExternalTable()` but we define it here as
   * `MetaStoreUtils.isExternalTable()` isn't available in old versions of Hive.
   */
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

  public static String getDbTableName(Table table) {
    return table.getDbName() + "." + table.getTableName();
  }

  public static boolean enableCommitterInTez(Configuration conf) {
    String tezCommitter = conf.get("hive.tez.mapreduce.output.committer.class");
    return (tezCommitter != null
        && tezCommitter.equals("org.apache.tez.mapreduce.committer.MROutputCommitter"));
  }

  public static TaskAttemptID getHiveTaskAttemptIDWrapper(Configuration conf) {
    return new HiveTaskAttemptIDWrapper(
        TaskAttemptID.forName(conf.get("mapred.task.id")), conf.get("hive.tez.vertex.index"));
  }

  private static JobID getJobIDWithVertexAppended(JobID jobID, String vertexId) {
    if (vertexId != null && !vertexId.isEmpty()) {
      return new JobID(jobID.getJtIdentifier() + vertexId, jobID.getId());
    } else {
      return jobID;
    }
  }

  public static String getTaskID(Configuration conf) {
    if (isSparkJob(conf)) {
      return getSparkTaskID();
    }
    return getHiveTaskAttemptIDWrapper(conf).getTaskID().toString();
  }

  /**
   * Retrieves the Spark task ID from the Spark context. Uses reflection to avoid this library
   * requiring dependencies on Spark packages.
   */
  public static String getSparkTaskID() {
    try {
      Class<?> taskContextClass = Class.forName("org.apache.spark.TaskContext");
      Method getMethod = taskContextClass.getMethod("get");
      Object taskContext = getMethod.invoke(null);
      Method partitionIdMethod = taskContextClass.getMethod("partitionId");
      Object partitionId = partitionIdMethod.invoke(taskContext);
      Method stageIdMethod = taskContextClass.getMethod("stageId");
      Object stageId = stageIdMethod.invoke(taskContext);
      return String.format("stage-%s-partition-%s", stageId, partitionId);
    } catch (ClassNotFoundException
        | InvocationTargetException
        | NoSuchMethodException
        | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private static class HiveTaskAttemptIDWrapper extends TaskAttemptID {

    HiveTaskAttemptIDWrapper(TaskAttemptID taskAttemptID, String vertexId) {
      super(
          getJobIDWithVertexAppended(taskAttemptID.getJobID(), vertexId).getJtIdentifier(),
          taskAttemptID.getJobID().getId(),
          taskAttemptID.getTaskType(),
          taskAttemptID.getTaskID().getId(),
          taskAttemptID.getId());
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof HiveTaskAttemptIDWrapper)) {
        return false;
      }
      HiveTaskAttemptIDWrapper other = (HiveTaskAttemptIDWrapper) obj;
      return (getId() == other.getId()
          && getTaskID().getId() == other.getTaskID().getId()
          && Objects.equals(getJobID(), other.getJobID()));
    }

    @Override
    public int hashCode() {
      return Objects.hash(getId(), getTaskID().getId(), getJobID());
    }
  }
}
