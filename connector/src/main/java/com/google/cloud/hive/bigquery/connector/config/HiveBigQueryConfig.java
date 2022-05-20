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
package com.google.cloud.hive.bigquery.connector.config;

import static com.google.cloud.bigquery.connector.common.BigQueryUtil.firstPresent;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobInfo.CreateDisposition;
import com.google.cloud.bigquery.JobInfo.SchemaUpdateOption;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.TimePartitioning.Type;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryConfig;
import com.google.cloud.bigquery.connector.common.BigQueryProxyConfig;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfig;
import com.google.cloud.bigquery.connector.common.ReadSessionCreatorConfigBuilder;
import com.google.cloud.bigquery.storage.v1.ArrowSerializationOptions.CompressionCodec;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.hive.bigquery.connector.utils.HiveUtils;
import java.io.Serializable;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import org.apache.hadoop.conf.Configuration;
import org.threeten.bp.Duration;
import repackaged.by.hivebqconnector.com.google.common.base.Optional;
import repackaged.by.hivebqconnector.com.google.common.collect.ImmutableList;
import repackaged.by.hivebqconnector.com.google.common.collect.ImmutableMap;

/** Main config class to interact with the bigquery-common-connector. */
public class HiveBigQueryConfig
    implements BigQueryConfig, BigQueryClient.LoadDataOptions, Serializable {

  public static final int DEFAULT_CACHE_EXPIRATION_IN_MINUTES = 15;
  private static final int DEFAULT_BIGQUERY_CLIENT_CONNECT_TIMEOUT = 60 * 1000;
  private static final int DEFAULT_BIGQUERY_CLIENT_READ_TIMEOUT = 60 * 1000;
  private static final int DEFAULT_BIGQUERY_CLIENT_RETRIES = 10;
  static final String GCS_CONFIG_CREDENTIALS_FILE_PROPERTY =
      "google.cloud.auth.service.account.json.keyfile";
  public static final int DEFAULT_MATERIALIZATION_EXPRIRATION_TIME_IN_MINUTES = 24 * 60;
  public static final String VIEWS_ENABLED_OPTION = "viewsEnabled";

  private TableId tableId;
  private Optional<String> credentialsKey = empty();
  private Optional<String> credentialsFile = empty();
  private Optional<String> accessToken = empty();
  private Optional<String> traceId = empty();

  /*
   * Used for "file-load" write jobs.
   * Indicates whether to interpret Avro logical types as the corresponding BigQuery data
   * type (for example, TIMESTAMP), instead of using the raw type (for example, LONG).
   */
  boolean useAvroLogicalTypes = true;

  // ARROW or AVRO
  DataFormat readDataFormat;

  // Options currently not implemented:
  HiveBigQueryProxyConfig proxyConfig;
  boolean viewsEnabled = false;
  Optional<String> materializationProject = empty();
  Optional<String> materializationDataset = empty();
  Optional<String> partitionField = empty();
  Optional<Type> partitionType = empty();
  Long partitionExpirationMs = null;
  Optional<Boolean> partitionRequireFilter = empty();
  Optional<String[]> clusteredFields = empty();
  ImmutableList<JobInfo.SchemaUpdateOption> loadSchemaUpdateOptions = ImmutableList.of();
  private Optional<String> storageReadEndpoint = empty();
  private ImmutableMap<String, String> bigQueryJobLabels = ImmutableMap.of();
  String parentProjectId;
  boolean useParentProjectForMetadataOperations;
  int materializationExpirationTimeInMinutes = DEFAULT_MATERIALIZATION_EXPRIRATION_TIME_IN_MINUTES;
  int maxReadRowsRetries = 3;
  Integer maxParallelism = null;
  private Optional<String> encodedCreateReadSessionRequest = empty();
  private int numBackgroundThreadsPerStream = 0;
  boolean pushAllFilters = true;
  private int numPrebufferReadRowsResponses = MIN_BUFFERED_RESPONSES_PER_STREAM;
  public static final int MIN_BUFFERED_RESPONSES_PER_STREAM = 1;
  private int numStreamsPerPartition = MIN_STREAMS_PER_PARTITION;
  public static final int MIN_STREAMS_PER_PARTITION = 1;
  private CompressionCodec arrowCompressionCodec = DEFAULT_ARROW_COMPRESSION_CODEC;
  static final CompressionCodec DEFAULT_ARROW_COMPRESSION_CODEC =
      CompressionCodec.COMPRESSION_UNSPECIFIED;

  HiveBigQueryConfig() {
    // empty
  }

  public static HiveBigQueryConfig from(Configuration conf, TableId tableId) {
    HiveBigQueryConfig config = new HiveBigQueryConfig();
    config.tableId = tableId;
    config.proxyConfig = HiveBigQueryProxyConfig.from(conf);
    String serde = RunConf.Config.READ_DATA_FORMAT.get(conf);
    if (serde.equals(RunConf.ARROW)) {
      config.readDataFormat = DataFormat.ARROW;
    } else if (serde.equals(RunConf.AVRO)) {
      config.readDataFormat = DataFormat.AVRO;
    } else {
      throw new RuntimeException("Invalid input SerDe type: " + serde);
    }
    // TODO: Should we add the "bq." prefix to the "credentials", "credentialsFile", and
    //  "gcpAccessToken" keys?
    config.credentialsKey = Optional.fromNullable(conf.get("credentials"));
    config.credentialsFile =
        Optional.fromJavaUtil(
            firstPresent(
                Optional.fromNullable(conf.get("credentialsFile")).toJavaUtil(),
                Optional.fromNullable(conf.get(GCS_CONFIG_CREDENTIALS_FILE_PROPERTY))
                    .toJavaUtil()));
    config.accessToken = Optional.fromNullable(conf.get("gcpAccessToken"));
    config.traceId = Optional.of("Hive:" + HiveUtils.getHiveId(conf));
    return config;
  }

  private static Optional empty() {
    return Optional.absent();
  }

  @Override
  public TableId getTableId() {
    return tableId;
  }

  @Override
  public java.util.Optional<CreateDisposition> getCreateDisposition() {
    return java.util.Optional.empty();
  }

  @Override
  public java.util.Optional<String> getPartitionField() {
    return partitionField.toJavaUtil();
  }

  @Override
  public java.util.Optional<Type> getPartitionType() {
    return partitionType.toJavaUtil();
  }

  @Override
  public Type getPartitionTypeOrDefault() {
    return partitionType.or(TimePartitioning.Type.DAY);
  }

  @Override
  public OptionalLong getPartitionExpirationMs() {
    return partitionExpirationMs == null
        ? OptionalLong.empty()
        : OptionalLong.of(partitionExpirationMs);
  }

  @Override
  public java.util.Optional<Boolean> getPartitionRequireFilter() {
    return partitionRequireFilter.toJavaUtil();
  }

  @Override
  public java.util.Optional<ImmutableList<String>> getClusteredFields() {
    return clusteredFields.transform(fields -> ImmutableList.copyOf(fields)).toJavaUtil();
  }

  @Override
  public boolean isUseAvroLogicalTypes() {
    return useAvroLogicalTypes;
  }

  @Override
  public List<SchemaUpdateOption> getLoadSchemaUpdateOptions() {
    return loadSchemaUpdateOptions;
  }

  @Override
  public java.util.Optional<String> getCredentialsKey() {
    return credentialsKey.toJavaUtil();
  }

  @Override
  public java.util.Optional<String> getCredentialsFile() {
    return credentialsFile.toJavaUtil();
  }

  @Override
  public java.util.Optional<String> getAccessToken() {
    return accessToken.toJavaUtil();
  }

  @Override
  public String getParentProjectId() {
    return parentProjectId;
  }

  @Override
  public boolean useParentProjectForMetadataOperations() {
    return useParentProjectForMetadataOperations;
  }

  @Override
  public boolean isViewsEnabled() {
    return viewsEnabled;
  }

  @Override
  public java.util.Optional<String> getMaterializationProject() {
    return materializationProject.toJavaUtil();
  }

  @Override
  public java.util.Optional<String> getMaterializationDataset() {
    return materializationDataset.toJavaUtil();
  }

  @Override
  public int getBigQueryClientConnectTimeout() {
    return DEFAULT_BIGQUERY_CLIENT_CONNECT_TIMEOUT; // TODO: Make configurable
  }

  @Override
  public int getBigQueryClientReadTimeout() {
    return DEFAULT_BIGQUERY_CLIENT_READ_TIMEOUT; // TODO: Make configurable
  }

  @Override
  public RetrySettings getBigQueryClientRetrySettings() {
    return RetrySettings.newBuilder()
        .setMaxAttempts(DEFAULT_BIGQUERY_CLIENT_RETRIES) // TODO: Make configurable
        .setTotalTimeout(Duration.ofMinutes(10))
        .setInitialRpcTimeout(Duration.ofSeconds(60))
        .setMaxRpcTimeout(Duration.ofMinutes(5))
        .setRpcTimeoutMultiplier(1.6)
        .setRetryDelayMultiplier(1.6)
        .setInitialRetryDelay(Duration.ofMillis(1250))
        .setMaxRetryDelay(Duration.ofSeconds(5))
        .build();
  }

  @Override
  public BigQueryProxyConfig getBigQueryProxyConfig() {
    return proxyConfig;
  }

  @Override
  public java.util.Optional<String> getEndpoint() {
    return storageReadEndpoint.toJavaUtil();
  }

  @Override
  public int getCacheExpirationTimeInMinutes() {
    return DEFAULT_CACHE_EXPIRATION_IN_MINUTES; // TODO: Make configurable
  }

  public DataFormat getReadDataFormat() {
    return readDataFormat;
  }

  @Override
  public ImmutableMap<String, String> getBigQueryJobLabels() {
    return bigQueryJobLabels;
  }

  public OptionalInt getMaxParallelism() {
    return maxParallelism == null ? OptionalInt.empty() : OptionalInt.of(maxParallelism);
  }

  public Optional<String> getTraceId() {
    return traceId;
  }

  public ReadSessionCreatorConfig toReadSessionCreatorConfig() {
    return new ReadSessionCreatorConfigBuilder()
        .setViewsEnabled(viewsEnabled)
        .setMaterializationProject(materializationProject.toJavaUtil())
        .setMaterializationDataset(materializationDataset.toJavaUtil())
        .setMaterializationExpirationTimeInMinutes(materializationExpirationTimeInMinutes)
        .setReadDataFormat(readDataFormat)
        .setMaxReadRowsRetries(maxReadRowsRetries)
        .setViewEnabledParamName(VIEWS_ENABLED_OPTION)
        .setDefaultParallelism(1) // TODO: Make configurable?
        .setMaxParallelism(getMaxParallelism())
        .setRequestEncodedBase(encodedCreateReadSessionRequest.toJavaUtil())
        .setEndpoint(storageReadEndpoint.toJavaUtil())
        .setBackgroundParsingThreads(numBackgroundThreadsPerStream)
        .setPushAllFilters(pushAllFilters)
        .setPrebufferReadRowsResponses(numPrebufferReadRowsResponses)
        .setStreamsPerPartition(numStreamsPerPartition)
        .setArrowCompressionCodec(arrowCompressionCodec)
        .setTraceId(traceId.toJavaUtil())
        .build();
  }
}
