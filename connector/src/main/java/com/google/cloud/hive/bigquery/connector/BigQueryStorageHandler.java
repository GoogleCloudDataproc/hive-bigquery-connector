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
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
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

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    setGCSAccessTokenProvider(jobConf);
    WriteEntity writeEntity = HiveUtils.getWriteEntity(conf);
    if (writeEntity == null
        || (writeEntity.getWriteType() != WriteEntity.WriteType.INSERT
            && writeEntity.getWriteType() != WriteEntity.WriteType.INSERT_OVERWRITE)) {
      // This is not a write job, so we don't need to anything more here
      return;
    }

    JobDetails jobDetails = JobDetails.getJobDetails(conf);
    jobDetails.setProject(
        jobDetails.getTableProperties().get(HiveBigQueryConfig.PROJECT_KEY).toString());
    jobDetails.setDataset(
        jobDetails.getTableProperties().get(HiveBigQueryConfig.DATASET_KEY).toString());
    jobDetails.setTable(
        jobDetails.getTableProperties().get(HiveBigQueryConfig.TABLE_KEY).toString());
    jobDetails.setOverwrite(writeEntity.getWriteType() == WriteEntity.WriteType.INSERT_OVERWRITE);
    if (writeEntity.getPartition() != null) {
      jobDetails.setOutputPartitionValues(writeEntity.getPartition().getSpec());
    }

    String engine = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE);
    if (engine.equals("mr")) {
      // Only set the OutputCommitter class if we're dealing with an actual output job,
      // i.e. where data gets written to BigQuery. Otherwise, the "mr" engine will call
      // the OutputCommitter.commitJob() method even for some queries
      // (e.g. "select count(*)") that aren't actually supposed to output data.
      jobConf.set(Constants.HADOOP_COMMITTER_CLASS_KEY, BigQueryOutputCommitter.class.getName());
    } else if (engine.equals("tez")) {
      if (jobDetails.getOutputPartitionValues() != null) {
        throw new RuntimeException(
            "INSERT PARTITION statements are currently not supported with the `tez` engine");
      }
    } else {
      throw new RuntimeException("Unsupported execution engine: " + engine);
    }

    // Note: Unfortunately the table properties do not contain constraints like
    // "NOT NULL", so the inferred avro & proto schema assume that all columns are
    // optional (e.g. UNION["null", "long"]). So if these constraints are necessary
    // for the user, then the user should provide an explicit avro schema at table
    // creation time.
    // See: https://lists.apache.org/thread/mjm4yznf87xzbk7xywf2gvmnp3l1dm5d

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
      if (jobDetails.isOverwrite() && jobDetails.getOutputPartitionValues() != null) {
        throw new RuntimeException(
            String.format(
                "INSERT OVERWRITE PARTITION statements are currently not supported with the `%s`"
                    + " write method",
                HiveBigQueryConfig.WRITE_METHOD_INDIRECT));
      }

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
    // Retrieve the table properties here instead of in `configureJobConf()` because somehow
    // in `configureJobConf()` some properties (for example, "partition_columns") are missing
    // from `tableDesc.getProperties()`.
    JobDetails jobDetails = JobDetails.getJobDetails(conf);
    jobDetails.setTableProperties(tableDesc.getProperties());
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
