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
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConnectorModule;
import com.google.cloud.hive.bigquery.connector.input.BigQueryInputFormat;
import com.google.cloud.hive.bigquery.connector.output.BigQueryOutputCommitter;
import com.google.cloud.hive.bigquery.connector.output.BigQueryOutputFormat;
import com.google.cloud.hive.bigquery.connector.output.MapReduceOutputFormat;
import com.google.cloud.hive.bigquery.connector.sparksql.SparkSQLUtils;
import com.google.cloud.hive.bigquery.connector.utils.JobUtils;
import com.google.cloud.hive.bigquery.connector.utils.avro.AvroUtils;
import com.google.cloud.hive.bigquery.connector.utils.hive.HiveUtils;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
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
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputFormat;

/** Main entrypoint for Hive/BigQuery interactions. */
@SuppressWarnings({"rawtypes", "deprecated"})
public abstract class BigQueryStorageHandlerBase
    implements HiveStoragePredicateHandler, HiveStorageHandler {

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
    String engine = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).toLowerCase();
    if ((engine.equals("tez") && HiveUtils.enableCommitterInTez(conf))) {
      // This version of Hive enables tez committer HIVE-24629
      conf.set(HiveBigQueryConfig.HADOOP_COMMITTER_CLASS_KEY, NoOpCommitter.class.getName());
    } else if (engine.equals("mr")) {
      if (conf.get(HiveBigQueryConfig.THIS_IS_AN_OUTPUT_JOB, "false").equals("true")) {
        // Only set the OutputCommitter class if we're dealing with an actual output job,
        // i.e. where data gets written to BigQuery. Otherwise, the "mr" engine will call
        // the OutputCommitter.commitJob() method even for some queries
        // (e.g. "select count(*)") that aren't actually supposed to output data.
        jobConf.set(
            HiveBigQueryConfig.HADOOP_COMMITTER_CLASS_KEY, BigQueryOutputCommitter.class.getName());
      }
    }
    setOutputTables(tableDesc);
  }

  protected void setOutputTables(TableDesc tableDesc) {
    // Figure out the output table(s)
    String hmsDbTableName = tableDesc.getTableName();
    String tables = conf.get(HiveBigQueryConfig.OUTPUT_TABLES_KEY);
    tables =
        tables == null
            ? hmsDbTableName
            : tables + HiveBigQueryConfig.TABLE_NAME_SEPARATOR + hmsDbTableName;
    conf.set(HiveBigQueryConfig.OUTPUT_TABLES_KEY, tables);
  }

  /**
   * Committer with no-op job commit. Set this for Tez so it uses BigQueryMetaHook's
   * commitInsertTable to commit per table. For task commit/abort and job abort still use our
   * regular OutputCommitter.
   */
  static class NoOpCommitter extends BigQueryOutputCommitter {
    @Override
    public void commitJob(JobContext jobContext) throws IOException {
      // do nothing
    }
  }

  protected static void validateTempGcsPath(
      String tempGcsPath, BigQueryCredentialsSupplier credentialsSupplier) {
    if (tempGcsPath == null || tempGcsPath.trim().equals("")) {
      throw new RuntimeException(
          String.format(
              "The '%s' property must be set when using the '%s' write method.",
              HiveBigQueryConfig.TEMP_GCS_PATH_KEY, HiveBigQueryConfig.WRITE_METHOD_INDIRECT));
    } else if (!JobUtils.hasGcsWriteAccess(credentialsSupplier, tempGcsPath)) {
      throw new RuntimeException(
          String.format(
              "Does not have write access to the following GCS path, or bucket does not exist: %s",
              tempGcsPath));
    }
  }

  public static void configureJobDetailsForIndirectWrite(
      HiveBigQueryConfig opts,
      JobDetails jobDetails,
      BigQueryCredentialsSupplier credentialsSupplier) {
    // validate the temp GCS path to store the temporary Avro files
    validateTempGcsPath(opts.getTempGcsPath(), credentialsSupplier);
    // Convert BigQuery schema to Avro schema
    StructObjectInspector rowObjectInspector =
        BigQuerySerDe.getRowObjectInspector(jobDetails.getTableProperties());
    org.apache.avro.Schema avroSchema =
        AvroUtils.getAvroSchema(rowObjectInspector, jobDetails.getBigquerySchema().getFields());
    jobDetails.setAvroSchema(avroSchema);
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    Injector injector =
        Guice.createInjector(
            new BigQueryClientModule(),
            new HiveBigQueryConnectorModule(conf, tableDesc.getProperties()));
    BigQueryClient bqClient = injector.getInstance(BigQueryClient.class);
    HiveBigQueryConfig opts = injector.getInstance(HiveBigQueryConfig.class);
    Properties tableProperties = tableDesc.getProperties();
    String hmsDbTableName = tableDesc.getTableName();

    // A workaround for mr mode, as MapRedTask.execute resets mapred.output.committer.class
    conf.set(HiveBigQueryConfig.THIS_IS_AN_OUTPUT_JOB, "true");

    // Set config for the GCS Connector
    setGCSAccessTokenProvider(conf);

    // Retrieve some info from the BQ table
    TableId tableId =
        BigQueryUtil.parseTableId(tableProperties.getProperty(HiveBigQueryConfig.TABLE_KEY));
    TableInfo bqTableInfo = bqClient.getTable(tableId);
    if (bqTableInfo == null) {
      throw new RuntimeException("BigQuery table does not exist: " + tableId);
    }
    Schema bigQuerySchema = bqTableInfo.getDefinition().getSchema();

    // Save the job details file to disk
    JobDetails jobDetails = new JobDetails();
    jobDetails.setWriteMethod(opts.getWriteMethod());
    jobDetails.setBigquerySchema(bigQuerySchema);
    jobDetails.setJobTempOutputPath(
        new Path(JobUtils.getQueryTempOutputPath(conf, opts), hmsDbTableName));
    jobDetails.setTableProperties(tableProperties);
    jobDetails.setTableId(tableId);

    if (opts.getWriteMethod().equals(HiveBigQueryConfig.WRITE_METHOD_INDIRECT)) {
      configureJobDetailsForIndirectWrite(
          opts, jobDetails, injector.getInstance(BigQueryCredentialsSupplier.class));
    }

    // Special treatment for Spark
    if (SparkSQLUtils.isSparkJob(conf)) {
      // Spark uses the new "mapreduce" Hadoop API for the job output format's committer
      conf.set("mapreduce.job.outputformat.class", MapReduceOutputFormat.class.getName());
      setOutputTables(tableDesc);
      if (SparkSQLUtils.isOverwrite(conf, tableDesc.getTableName())) {
        BigQueryMetaHookBase.makeOverwrite(conf, jobDetails);
      }
    }

    // Save the job details file to disk
    Path jobDetailsFilePath =
        JobUtils.getJobDetailsFilePath(conf, tableProperties.getProperty("name"));
    JobDetails.writeJobDetailsFile(conf, jobDetailsFilePath, jobDetails);
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    // Do nothing
  }

  @Override
  public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> map) {
    // Deprecated
  }
}
