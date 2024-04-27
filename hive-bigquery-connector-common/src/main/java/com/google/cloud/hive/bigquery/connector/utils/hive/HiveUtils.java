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

import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.utils.hcatalog.HCatalogUtils;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.JobConf;
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

  public static boolean isMRJob(JobConf job) {
    return job != null
        && (HiveConf.getVar(job, HiveConf.ConfVars.PLAN) != null)
        && (!HiveConf.getVar(job, HiveConf.ConfVars.PLAN).isEmpty());
  }

  /** Borrowed from Hive 2+ as it's not available in Hive 1. */
  public static String[] getReadColumnNames(Configuration conf) {
    String colNames = conf.get("hive.io.file.readcolumn.names", "");
    return colNames != null && !colNames.isEmpty() ? colNames.split(",") : new String[0];
  }

  /** Returns the query's unique id. */
  public static String getQueryId(Configuration conf) {
    if (!conf.get(HiveBigQueryConfig.QUERY_ID, "").isEmpty()) {
      return conf.get(HiveBigQueryConfig.QUERY_ID);
    }
    if (!conf.get(ConfVars.HIVEQUERYID.varname, "").isEmpty()) {
      // In this case, the user is running a plain Hive query directly from Hive itself.
      return "hive-query-id-" + conf.get(ConfVars.HIVEQUERYID.varname);
    } else if (!conf.get("pig.script.id", "").isEmpty()
        && !conf.get("pig.job.submitted.timestamp", "").isEmpty()) {
      // The user is running a Hive query from Pig. Use the job's timestamp as a pig script might
      // run multiple jobs.
      return String.format(
          "pig-%s-%s", conf.get("pig.script.id"), conf.get("pig.job.submitted.timestamp"));
    } else if (!conf.get(HCatalogUtils.HCAT_OUTPUT_ID_HASH, "").isEmpty()) {
      // Possibly from Pig in Tez mode in some environments (e.g. Dataproc)
      return String.format("hcat-output-%s", conf.get(HCatalogUtils.HCAT_OUTPUT_ID_HASH));
    } else if (!conf.get("mapreduce.workflow.id", "").isEmpty()) {
      // Map reduce job, possibly from Pig in MR mode in some environments (e.g. Dataproc)
      return String.format("mapreduce-%s", conf.get("mapreduce.workflow.id"));
    }
    // Fall back: generate our own ID
    String queryId = String.format("custom-query-id-%s", UUID.randomUUID());
    conf.set(HiveBigQueryConfig.QUERY_ID, queryId);
    return queryId;
  }

  public static String getDbTableName(Table table) {
    return table.getDbName() + "." + table.getTableName();
  }

  public static String getDbTableName(org.apache.hadoop.hive.ql.metadata.Table table) {
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
    return getHiveTaskAttemptIDWrapper(conf).getTaskID().toString();
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
