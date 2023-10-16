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
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.JobID;

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

  /** Returns the ID of the Hive query as set by Hive in the configuration. */
  public static String getQueryId(Configuration conf) {
    return HiveConf.getVar(conf, HiveConf.ConfVars.HIVEQUERYID);
  }

  public static String getDbTableName(org.apache.hadoop.hive.metastore.api.Table table) {
    return table.getDbName() + "." + table.getTableName();
  }

  public static boolean enableCommitterInTez(Configuration conf) {
    String tezCommitter = conf.get("hive.tez.mapreduce.output.committer.class");
    return (tezCommitter != null
        && tezCommitter.equals("org.apache.tez.mapreduce.committer.MROutputCommitter"));
  }

  public static TaskAttemptID taskAttemptIDWrapper(JobConf jobConf) {
    return new TaskAttemptIDWrapper(
        TaskAttemptID.forName(jobConf.get("mapred.task.id")), jobConf.get("hive.tez.vertex.index"));
  }

  private static JobID getJobIDWithVertexAppended(JobID jobID, String vertexId) {
    if (vertexId != null && !vertexId.isEmpty()) {
      return new JobID(jobID.getJtIdentifier() + vertexId, jobID.getId());
    } else {
      return jobID;
    }
  }

  private static class TaskAttemptIDWrapper extends TaskAttemptID {

    TaskAttemptIDWrapper(TaskAttemptID taskAttemptID, String vertexId) {
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
      if (!(obj instanceof TaskAttemptIDWrapper)) {
        return false;
      }
      TaskAttemptIDWrapper other = (TaskAttemptIDWrapper) obj;
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
