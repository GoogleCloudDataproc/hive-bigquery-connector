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
package com.google.cloud.hive.bigquery.connector;

import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientModule;
import com.google.cloud.bigquery.connector.common.BigQueryCredentialsSupplier;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConnectorModule;
import com.google.cloud.hive.bigquery.connector.input.BigQueryInputFormat;
import com.google.cloud.hive.bigquery.connector.output.BigQueryOutputCommitter;
import com.google.cloud.hive.bigquery.connector.output.BigQueryOutputFormat;
import com.google.cloud.hive.bigquery.connector.output.indirect.IndirectUtils;
import com.google.cloud.hive.bigquery.connector.utils.hive.HiveUtils;
import com.google.common.collect.Maps;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;

/** Main entrypoint for Hive/BigQuery interactions. */
@SuppressWarnings({"rawtypes", "deprecated"})
public class BigQueryStorageHandler implements HiveStoragePredicateHandler, HiveStorageHandler {

  Configuration conf;

  /** Configure the GCS connector to use the Hive connector's credentials. */
  public static void setGCSAccessTokenProvider(Configuration conf) {
    conf.set("fs.gs.auth.type", "ACCESS_TOKEN_PROVIDER");
    conf.set(
        "fs.gs.auth.access.token.provider",
        "com.google.cloud.hive.bigquery.connector.GCSConnectorAccessTokenProvider");
    conf.set(
        "fs.gs.auth.access.token.provider.impl",
        "com.google.cloud.hive.bigquery.connector.GCSConnectorAccessTokenProvider");
  }

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return BigQueryInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return BigQueryOutputFormat.class;
  }

  @Override
  public Class<? extends AbstractSerDe> getSerDeClass() {
    return BigQuerySerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return new BigQueryMetaHook(conf);
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
    return new DefaultHiveAuthorizationProvider();
  }

  @Override
  public DecomposedPredicate decomposePredicate(
      JobConf jobConf, Deserializer deserializer, ExprNodeDesc exprNodeDesc) {
    // TODO: See if we can dissociate the pushed predicates from the residual ones
    DecomposedPredicate predicate = new DecomposedPredicate();
    predicate.residualPredicate = (ExprNodeGenericFuncDesc) exprNodeDesc;
    predicate.pushedPredicate = (ExprNodeGenericFuncDesc) exprNodeDesc;
    return predicate;
  }

  @Override
  public void setConf(Configuration configuration) {
    this.conf = configuration;
    setGCSAccessTokenProvider(this.conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  private static List<FieldSchema> sanitizeColumns(List<FieldSchema> columns) {
    List<FieldSchema> sanitized = new ArrayList<>();
    for (FieldSchema column : columns) {
      FieldSchema clone = column.deepCopy();
      if (clone.getName().contains(".")) {
        clone.setName(clone.getName().split("\\.")[1]);
      }
      sanitized.add(clone);
    }
    return sanitized;
  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    setGCSAccessTokenProvider(jobConf);
    JobDetails jobDetails = JobDetails.getJobDetails(conf);

    SemanticAnalyzer analyzer = HiveUtils.getQueryAnalyzer(conf);
    Map<String, String> jobTableProps =
        new HashMap<>(Maps.fromProperties(jobDetails.getTableProperties()));
    if (jobTableProps.get("name").equals(tableDesc.getProperties().get("name"))
        && analyzer.getQueryProperties().isCTAS()) {
      try {
        jobDetails.setCTAS(true);
        JobDetails.saveJobDetails(conf, jobDetails);
        TableInfo tableInfo =
            BigQueryMetaHook.preCreateTable(
                conf, sanitizeColumns(analyzer.getResultSchema()), jobTableProps);
        BigQueryMetaHook.commitCreateTable(conf, jobTableProps, tableInfo);
        Properties newProps = new Properties();
        newProps.putAll(jobTableProps);
        tableDesc.setProperties(newProps);
      } catch (MetaException e) {
        throw new RuntimeException(e);
      }
    }

    WriteEntity writeEntity = HiveUtils.getWriteEntity(analyzer);
    if ((conf.get(Constants.THIS_IS_AN_OUTPUT_JOB, "false").equals("false"))
        || writeEntity == null
        || (writeEntity.getWriteType() != WriteEntity.WriteType.INSERT
            && writeEntity.getWriteType() != WriteEntity.WriteType.INSERT_OVERWRITE)) {
      // This is not a write job, so we don't need to anything more here
      return;
    }

    jobDetails.setOverwrite(writeEntity.getWriteType() == WriteEntity.WriteType.INSERT_OVERWRITE);

    String engine = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE);
    if (engine.equals("mr")) {
      // Only set the OutputCommitter class if we're dealing with an actual output job,
      // i.e. where data gets written to BigQuery. Otherwise, the "mr" engine will call
      // the OutputCommitter.commitJob() method even for some queries
      // (e.g. "select count(*)") that aren't actually supposed to output data.
      jobConf.set(Constants.HADOOP_COMMITTER_CLASS_KEY, BigQueryOutputCommitter.class.getName());
    }

    String writeMethod =
        conf.get(HiveBigQueryConfig.WRITE_METHOD_KEY, HiveBigQueryConfig.WRITE_METHOD_DIRECT);
    Injector injector =
        Guice.createInjector(
            new BigQueryClientModule(),
            new HiveBigQueryConnectorModule(conf, jobDetails.getTableProperties()));
    if (writeMethod.equals(HiveBigQueryConfig.WRITE_METHOD_DIRECT)) {
      // Get an instance of the BigQuery client
      BigQueryClient bqClient = injector.getInstance(BigQueryClient.class);

      // Retrieve the BigQuery schema of the final destination table
      Schema bigQuerySchema =
          bqClient.getTable(jobDetails.getTableId()).getDefinition().getSchema();

      // Special case: 'INSERT OVERWRITE' operation while using the 'direct'
      // write method. In this case, we will stream-write to a temporary table
      // and then finally overwrite the final destination table with the temporary
      // table's contents. This special case doesn't apply to the 'indirect'
      // write method, which doesn't need a temporary table -- instead that method
      // uses the 'WRITE_TRUNCATE' option available in the BigQuery Load Job API when
      // loading the Avro files into the BigQuery table (see more about that in the
      // `IndirectOutputCommitter` class).
      if (writeEntity.getWriteType() == WriteEntity.WriteType.INSERT_OVERWRITE) {
        // Set the final destination table as the job's original table
        jobDetails.setFinalTable(jobDetails.getTableId().getTable());
        // Create a temporary table with the same schema
        // TODO: It'd be useful to add a description to the table explaining that it was
        //  created as a temporary table for a Hive query.
        TableInfo tableInfo =
            bqClient.createTempTable(
                TableId.of(
                    jobDetails.getProject(),
                    jobDetails.getDataset(),
                    jobDetails.getTableId().getTable() + "-" + HiveUtils.getHiveId(conf) + "-"),
                bigQuerySchema);
        // Set the temp table as the job's output table
        jobDetails.setTable(tableInfo.getTableId().getTable());
      }
    } else if (writeMethod.equals(HiveBigQueryConfig.WRITE_METHOD_INDIRECT)) {
      String tempGcsPath = conf.get(HiveBigQueryConfig.TEMP_GCS_PATH_KEY);
      jobDetails.setGcsTempPath(tempGcsPath);
      if (tempGcsPath == null || tempGcsPath.trim().equals("")) {
        throw new RuntimeException(
            String.format(
                "The '%s' property must be set when using the '%s' write method.",
                HiveBigQueryConfig.TEMP_GCS_PATH_KEY, HiveBigQueryConfig.WRITE_METHOD_INDIRECT));
      } else if (!IndirectUtils.hasGcsWriteAccess(
          injector.getInstance(BigQueryCredentialsSupplier.class), tempGcsPath)) {
        throw new RuntimeException(
            String.format(
                "Cannot write to table '%s'. Does not have write access to the"
                    + " following GCS path, or bucket does not exist: %s",
                tableDesc.getTableName(), tempGcsPath));
      }
    } else {
      throw new RuntimeException("Invalid write method: " + writeMethod);
    }

    // Save the info file so that we can retrieve all the information at later
    // stages of the job's execution
    JobDetails.saveJobDetails(conf, jobDetails);
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    conf.set(Constants.THIS_IS_AN_OUTPUT_JOB, "true");
    JobDetails jobDetails = new JobDetails();
    Properties tableProperties = tableDesc.getProperties();
    jobDetails.setTableProperties(tableProperties);
    jobDetails.setProject(tableProperties.get(HiveBigQueryConfig.PROJECT_KEY).toString());
    jobDetails.setDataset(tableProperties.get(HiveBigQueryConfig.DATASET_KEY).toString());
    jobDetails.setTable(tableProperties.get(HiveBigQueryConfig.TABLE_KEY).toString());
    JobDetails.saveJobDetails(conf, jobDetails);
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    // Do nothing
  }

  @Override
  public void configureInputJobCredentials(TableDesc tableDesc, Map<String, String> map) {
    // Do nothing
  }

  @Override
  public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> map) {
    // Do nothing
  }
}
