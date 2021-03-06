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

import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConnectorModule;
import com.google.cloud.hive.bigquery.connector.input.arrow.BigQueryArrowInputFormat;
import com.google.cloud.hive.bigquery.connector.input.avro.BigQueryAvroInputFormat;
import com.google.cloud.hive.bigquery.connector.output.BigQueryOutputCommitter;
import com.google.cloud.hive.bigquery.connector.output.BigQueryOutputFormat;
import com.google.cloud.hive.bigquery.connector.utils.proto.ProtoSchemaConverter;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.mapred.*;
import repackaged.by.hivebqconnector.com.google.protobuf.Descriptors;

/** Main entrypoint for Hive/BigQuery interactions. */
@SuppressWarnings({"rawtypes", "deprecated"})
public class BigQueryStorageHandler implements HiveStoragePredicateHandler, HiveStorageHandler {

  Configuration conf;

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    Injector injector = Guice.createInjector(new HiveBigQueryConnectorModule(conf));
    DataFormat readDataFormat = injector.getInstance(HiveBigQueryConfig.class).getReadDataFormat();
    if (readDataFormat.equals(DataFormat.ARROW)) {
      return BigQueryArrowInputFormat.class;
    } else if (readDataFormat.equals(DataFormat.AVRO)) {
      return BigQueryAvroInputFormat.class;
    }
    throw new RuntimeException("Invalid readDataFormat: " + readDataFormat);
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
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    String engine = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE);
    if (engine.equals("mr")) {
      // The OutputCommitter class is only used by the "mr" engine, not "tez".
      if (conf.get(Constants.THIS_IS_AN_OUTPUT_JOB, "false").equals("true")) {
        // Only set the OutputCommitter class if we're dealing with an actual output job,
        // i.e. where data gets written to BigQuery. Otherwise, the "mr" engine will call
        // the OutputCommitter.commitJob() method even for some queries
        // (e.g. "select count(*)") that aren't actually supposed to output data.
        jobConf.set(Constants.HADOOP_COMMITTER_CLASS_KEY, BigQueryOutputCommitter.class.getName());
      }
    }
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    conf.set(Constants.THIS_IS_AN_OUTPUT_JOB, "true");
    JobInfo jobInfo = new JobInfo();

    Properties tableProperties = tableDesc.getProperties();
    jobInfo.setTableProperties(tableProperties);
    String columnNameProperty = tableProperties.getProperty(serdeConstants.LIST_COLUMNS);
    String columnTypeProperty = tableProperties.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    String columnCommentProperty = tableProperties.getProperty("columns.comments", "");
    String columnNameDelimiter =
        tableProperties.containsKey(serdeConstants.COLUMN_NAME_DELIMITER)
            ? tableProperties.getProperty(serdeConstants.COLUMN_NAME_DELIMITER)
            : String.valueOf(',');

    String writeMethod =
        conf.get(HiveBigQueryConfig.WRITE_METHOD_KEY, HiveBigQueryConfig.WRITE_METHOD_DIRECT);
    if (writeMethod.equals(HiveBigQueryConfig.WRITE_METHOD_DIRECT)) {
      // Figure out the table's proto schema
      StructObjectInspector rowObjectInspector =
          BigQuerySerDe.getRowObjectInspector(tableProperties);
      Descriptors.Descriptor descriptor;
      try {
        descriptor = ProtoSchemaConverter.toDescriptor(rowObjectInspector);
      } catch (Descriptors.DescriptorValidationException e) {
        throw new RuntimeException(e);
      }
      ProtoSchema protoSchema =
          com.google.cloud.bigquery.storage.v1.ProtoSchemaConverter.convert(descriptor);
      jobInfo.setProtoSchema(protoSchema.toByteArray());
    } else {
      throw new RuntimeException("Invalid write method: " + writeMethod);
    }

    // Save the job info file to HDFS
    JobInfo.writeInfoFile(conf, jobInfo);
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
