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
package com.google.cloud.hive.bigquery.connector.output;

import com.google.cloud.hive.bigquery.connector.BigQueryMetaHook;
import com.google.cloud.hive.bigquery.connector.JobDetails;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.utils.FileSystemUtils;
import com.google.cloud.hive.bigquery.connector.utils.JobUtils;
import java.io.IOException;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.hooks.Entity.Type;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.QBParseInfo;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class PreInsertHook implements ExecuteWithHookContext {

  @Override
  public void run(HookContext hookContext) throws Exception {
    // First, check if we're indeed processing a BigQuery table
    boolean processingBqTable = false;
    for (WriteEntity entity : hookContext.getOutputs()) {
      if (entity.getType() == Type.TABLE
          && entity.getTable().getStorageHandler() != null
          && entity
              .getTable()
              .getStorageHandler()
              .getClass()
              .getName()
              .equals("com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler")) {
        processingBqTable = true;
        break;
      }
    }
    if (!processingBqTable) {
      // Not a BigQuery table, so we bail
      return;
    }

    // Parse and analyze the semantics of the Hive query.
    // We have to do this because unfortunately the WriteEntity objects in hookContext.getOutputs()
    // are systematically marked as being of type INSERT_OVERWRITE, regardless of whether it is
    // an "INSERT OVERWRITE" query or a regular "INSERT" query. This is apparently caused by the
    // fact that Hive 1.x.x treats all "non native" tables (i.e. by Hive 1.x.x's definition all
    // tables that have a storage handler defined:
    // https://github.com/apache/hive/blob/release-1.2.1/ql/src/java/org/apache/hadoop/hive/ql/metadata/Table.java#L845)
    // as INSERT_OVERWRITE:
    // https://github.com/apache/hive/blob/release-1.2.1/ql/src/java/org/apache/hadoop/hive/ql/parse/SemanticAnalyzer.java#L12147
    // To get around this issue, we parse the query ourselves and try to determine the proper type
    // for our purposes (insert or insert overwrite).
    QBParseInfo parseInfo;
    try {
      Configuration conf = hookContext.getConf();
      Context context = new Context(conf);
      context.setCmd(hookContext.getQueryPlan().getQueryString());
      ParseDriver parseDriver = new ParseDriver();
      ASTNode tree = parseDriver.parse(hookContext.getQueryPlan().getQueryString(), context);
      HiveConf hiveConf = new HiveConf(conf, this.getClass());
      SemanticAnalyzer analyzer = new SemanticAnalyzer(hiveConf);
      if (tree.getChildren().isEmpty() || tree.getChild(0).getType() != HiveParser.TOK_QUERY) {
        return;
      }
      analyzer.analyze((ASTNode) tree.getChild(0), context);
      parseInfo = analyzer.getQB().getParseInfo();
    } catch (IOException | ParseException | SemanticException e) {
      throw new RuntimeException(e);
    }
    // Search for JobDetails files in the work directory
    Path workDir = JobUtils.getQueryWorkDir(hookContext.getConf());
    Set<Path> jobDetailsFiles =
        FileSystemUtils.findFilesRecursively(
            hookContext.getConf(), workDir, HiveBigQueryConfig.JOB_DETAILS_FILE);
    // Determine whether those JobDetails should be marked as "overwrite" jobs based
    // on the parsed Hive query
    BigQueryMetaHook metahook = new BigQueryMetaHook(hookContext.getConf());
    for (Path jobDetailsFile : jobDetailsFiles) {
      JobDetails jobDetails = JobDetails.readJobDetailsFile(hookContext.getConf(), jobDetailsFile);
      String tableName = (String) jobDetails.getTableProperties().get("name");
      boolean overwrite = !parseInfo.isInsertIntoTable(tableName);
      metahook.preInsertTable(tableName, overwrite);
    }
  }
}
