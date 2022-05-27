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
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConnectorModule;
import com.google.cloud.hive.bigquery.connector.input.arrow.BigQueryArrowInputFormat;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
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
import org.apache.hadoop.mapred.*;

/** Main entrypoint for Hive/BigQuery interactions. */
@SuppressWarnings({"rawtypes", "deprecated"})
public class BigQueryStorageHandler implements HiveStoragePredicateHandler, HiveStorageHandler {

  Configuration conf;

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    Injector injector =
        Guice.createInjector(new HiveBigQueryConnectorModule(conf, Optional.empty()));
    DataFormat readDataFormat = injector.getInstance(HiveBigQueryConfig.class).getReadDataFormat();
    if (readDataFormat.equals(DataFormat.ARROW)) {
      return BigQueryArrowInputFormat.class;
    } else {
      throw new RuntimeException("Invalid readDataFormat: " + readDataFormat);
    }
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return TextOutputFormat.class; // Dummy input format. Will be replaced later.
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
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {}

  @Override
  public void configureOutputJobProperties(
      TableDesc tableDesc, Map<String, String> jobProperties) {}

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
