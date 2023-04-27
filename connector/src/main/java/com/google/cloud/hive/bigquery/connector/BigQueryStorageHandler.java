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

import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientModule;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConnectorModule;
import com.google.cloud.hive.bigquery.connector.input.BigQueryInputFormat;
import com.google.cloud.hive.bigquery.connector.output.BigQueryOutputCommitter;
import com.google.cloud.hive.bigquery.connector.output.BigQueryOutputFormat;
import com.google.cloud.hive.bigquery.connector.utils.JobUtils;
import com.google.cloud.hive.bigquery.connector.utils.bq.BigQueryUtils;
import com.google.cloud.hive.bigquery.connector.utils.hive.HiveUtils;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.stats.Partish;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
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
    HiveBigQueryConfig.supportOldTableProperties(tableDesc.getProperties());
    String engine = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).toLowerCase();
    if (engine.equals("mr")) {
      if (conf.get(HiveBigQueryConfig.THIS_IS_AN_OUTPUT_JOB, "false").equals("true")) {
        // Only set the OutputCommitter class if we're dealing with an actual output job,
        // i.e. where data gets written to BigQuery. Otherwise, the "mr" engine will call
        // the OutputCommitter.commitJob() method even for some queries
        // (e.g. "select count(*)") that aren't actually supposed to output data.
        jobConf.set(
            HiveBigQueryConfig.HADOOP_COMMITTER_CLASS_KEY, BigQueryOutputCommitter.class.getName());
      }
    }
    String hmsDbTableName = tableDesc.getProperties().getProperty("name");
    String tables = jobConf.get(HiveBigQueryConfig.OUTPUT_TABLES_KEY);
    tables =
        tables == null
            ? hmsDbTableName
            : tables + HiveBigQueryConfig.TABLE_NAME_SEPARATOR + hmsDbTableName;
    jobConf.set(HiveBigQueryConfig.OUTPUT_TABLES_KEY, tables);
    setGCSAccessTokenProvider(jobConf);
  }

  /**
   * Committer with no-op job commit. Set this for Tez so it uses BigQueryMetaHook's
   * commitInsertTable to commit per table. For task commit/abort and job abort still use
   * BigQueryOutputCommitter.
   */
  static class BigQueryNoJobCommitter extends BigQueryOutputCommitter {
    @Override
    public void commitJob(JobContext jobContext) throws IOException {
      // do nothing
    }
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    HiveBigQueryConfig.supportOldTableProperties(tableDesc.getProperties());
    // A workaround for mr mode, as MapRedTask.execute resets mapred.output.committer.class
    conf.set(HiveBigQueryConfig.THIS_IS_AN_OUTPUT_JOB, "true");

    if (HiveUtils.enableCommitterInTez(conf)) {
      // This version Hive enables tez committer HIVE-24629
      conf.set(
          HiveBigQueryConfig.HADOOP_COMMITTER_CLASS_KEY, BigQueryNoJobCommitter.class.getName());
    }
    JobDetails jobDetails = new JobDetails();
    Properties tableProperties = tableDesc.getProperties();
    jobDetails.setTableProperties(tableProperties);
    jobDetails.setTableId(
        BigQueryUtil.parseTableId(tableProperties.getProperty(HiveBigQueryConfig.TABLE_KEY)));
    Path jobDetailsFilePath =
        JobUtils.getJobDetailsFilePath(conf, tableProperties.getProperty("name"));
    JobDetails.writeJobDetailsFile(conf, jobDetailsFilePath, jobDetails);
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
    // Deprecated
  }

  /*
  The following API may not be available in Hive-3, check running Hive if they are available.
  */
  // @Override
  public boolean canProvideBasicStatistics() {
    return true;
  }

  // @Override
  public Map<String, String> getBasicStatistics(Partish partish) {
    org.apache.hadoop.hive.ql.metadata.Table hmsTable = partish.getTable();
    Injector injector =
        Guice.createInjector(
            new BigQueryClientModule(),
            new HiveBigQueryConnectorModule(conf, hmsTable.getParameters()));
    BigQueryClient bqClient = injector.getInstance(BigQueryClient.class);
    HiveBigQueryConfig config = injector.getInstance(HiveBigQueryConfig.class);
    return BigQueryUtils.getBasicStatistics(bqClient, config.getTableId());
  }
}
