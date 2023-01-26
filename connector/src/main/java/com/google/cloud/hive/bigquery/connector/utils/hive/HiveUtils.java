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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.parse.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.JobID;

/**
 * Helper class that looks up details about a task ID and Tez Vertex ID. This is useful to create
 * temporary files uniquely named after the individual tasks.
 */
public class HiveUtils {

  /** Returns the ID of the Hive query as set by Hive in the configuration. */
  public static String getHiveId(Configuration conf) {
    return HiveConf.getVar(conf, HiveConf.ConfVars.HIVEQUERYID);
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

  public static SemanticAnalyzer getQueryAnalyzer(Configuration conf) {
    HiveConf hiveConf = new HiveConf(conf, HiveConf.class);
    String query;
    try {
      query =
          java.net.URLDecoder.decode(conf.get(HiveConf.ConfVars.HIVEQUERYSTRING.varname), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
    ASTNode tree;
    try {
      tree = ParseUtils.parse(query);
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
    SemanticAnalyzer analyzer;
    QueryState queryState = new QueryState.Builder().withHiveConf(hiveConf).build();
    try {
      analyzer = (SemanticAnalyzer) SemanticAnalyzerFactory.get(queryState, tree);
      analyzer.analyze(tree, new Context(hiveConf));
    } catch (SemanticException | IOException e) {
      throw new RuntimeException(e);
    }
    return analyzer;
  }

  public static WriteEntity getWriteEntity(BaseSemanticAnalyzer analyzer) {
    HashSet<WriteEntity> allOutputs = analyzer.getAllOutputs();
    if (allOutputs.size() > 0) {
      return (WriteEntity) allOutputs.toArray()[0];
    } else {
      return null;
    }
  }
}
