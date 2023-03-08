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

import static shaded.hivebqcon.com.google.cloud.bigquery.connector.common.BigQueryUtil.firstPresent;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobInfo.CreateDisposition;
import com.google.cloud.bigquery.JobInfo.SchemaUpdateOption;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.storage.v1.ArrowSerializationOptions.CompressionCodec;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.hive.bigquery.connector.utils.hive.HiveUtils;
import java.io.Serializable;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.threeten.bp.Duration;
import shaded.hivebqcon.com.google.cloud.bigquery.connector.common.*;
import shaded.hivebqcon.com.google.common.base.Optional;
import shaded.hivebqcon.com.google.common.collect.ImmutableList;
import shaded.hivebqcon.com.google.common.collect.ImmutableMap;

/** Main config class to interact with the bigquery-common-connector. */
@SuppressWarnings("unchecked")
public class HiveBigQueryConfig
    implements BigQueryConfig, BigQueryClient.LoadDataOptions, Serializable {

  private static final long serialVersionUID = 1L;

  // Config keys
  public static final String PROJECT_KEY = "bq.project";
  public static final String DATASET_KEY = "bq.dataset";
  public static final String TABLE_KEY = "bq.table";
  public static final String WRITE_METHOD_KEY = "bq.write.method";
  public static final String TEMP_GCS_PATH_KEY = "bq.temp.gcs.path";
  public static final String WORK_DIR_PARENT_PATH_KEY = "bq.work.dir.parent.path";
  public static final String WORK_DIR_NAME_PREFIX_KEY = "bq.work.dir.name.prefix";
  public static final String READ_DATA_FORMAT_KEY = "bq.read.data.format";
  public static final String READ_CREATE_SESSION_TIMEOUT_KEY = "bq.read.create.session.timeout";
  public static final String READ_MAX_PARALLELISM = "maxParallelism";
  public static final String READ_PREFERRED_PARALLELISM = "preferredMinParallelism";
  public static final String CREDENTIALS_KEY_KEY = "bq.credentials.key";
  public static final String CREDENTIALS_FILE_KEY = "bq.credentials.file";
  public static final String ACCESS_TOKEN_KEY = "bq.access.token";
  public static final String ACCESS_TOKEN_PROVIDER_FQCN_KEY =
      "bq.access.access.token.provider.fqcn";
  public static final String CREATE_DISPOSITION_KEY = "bq.create.disposition";
  public static final String TIME_PARTITION_TYPE_KEY = "bq.time.partition.type";
  public static final String TIME_PARTITION_FIELD_KEY = "bq.time.partition.field";
  public static final String TIME_PARTITION_EXPIRATION_KEY = "bq.time.partition.expiration.ms";
  public static final String TIME_PARTITION_REQUIRE_FILTER_KEY = "bq.time.partition.require.filter";
  public static final String CLUSTERED_FIELDS_KEY = "bq.clustered.fields";
  public static final String VIEWS_ENABLED_KEY = "viewsEnabled";

  public static final int DEFAULT_CACHE_EXPIRATION_IN_MINUTES = 15;
  private static final int DEFAULT_BIGQUERY_CLIENT_CONNECT_TIMEOUT = 60 * 1000;
  private static final int DEFAULT_BIGQUERY_CLIENT_READ_TIMEOUT = 60 * 1000;
  private static final int DEFAULT_BIGQUERY_CLIENT_RETRIES = 10;
  static final String GCS_CONFIG_CREDENTIALS_FILE_PROPERTY =
      "google.cloud.auth.service.account.json.keyfile";
  public static final String WORK_DIR_NAME_PREFIX_DEFAULT = "bq-hive-";

  private TableId tableId;
  private Optional<String> columnNameDelimiter;
  private Optional<String> traceId = empty();

  // Credentials management
  private Optional<String> credentialsKey = empty();
  private Optional<String> credentialsFile = empty();
  private Optional<String> accessToken = empty();
  private Optional<String> accessTokenProviderFQCN;

  /*
   * Options used for "indirect" write jobs.
   */
  // Indicates whether to interpret Avro logical types as the corresponding BigQuery data
  // type (for example, TIMESTAMP), instead of using the raw type (for example, LONG).
  boolean useAvroLogicalTypes = true;
  ImmutableList<String> decimalTargetTypes = ImmutableList.of();

  // Reading parameters
  private DataFormat readDataFormat; // ARROW or AVRO
  private Optional<Long> createReadSessionTimeoutInSeconds;
  public static final String ARROW = "arrow";
  public static final String AVRO = "avro";

  // Views
  boolean viewsEnabled;
  Optional<String> materializationProject;
  Optional<String> materializationDataset;
  int materializationExpirationTimeInMinutes;

  // Writing parameters
  public static final String WRITE_METHOD_DIRECT = "direct";
  public static final String WRITE_METHOD_INDIRECT = "indirect";

  // Partitioning and clustering
  Optional<String> partitionField = empty();
  Optional<TimePartitioning.Type> partitionType = empty();
  Long partitionExpirationMs = null;
  Optional<Boolean> partitionRequireFilter = empty();
  Optional<String[]> clusteredFields = empty();

  // Options currently not implemented:
  HiveBigQueryProxyConfig proxyConfig;
  boolean enableModeCheckForSchemaFields = true;
  Optional<JobInfo.CreateDisposition> createDisposition = empty();
  ImmutableList<JobInfo.SchemaUpdateOption> loadSchemaUpdateOptions = ImmutableList.of();
  private ImmutableMap<String, String> bigQueryJobLabels = ImmutableMap.of();
  String parentProjectId;
  boolean useParentProjectForMetadataOperations;
  int maxReadRowsRetries = 3;
  Integer maxParallelism = null;
  Integer preferredMinParallelism = null;
  private Optional<String> encodedCreateReadSessionRequest = empty();
  private Optional<String> bigQueryStorageGrpcEndpoint = empty();
  private Optional<String> bigQueryHttpEndpoint = empty();
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

  private static Optional<String> getAnyOption(
      String key, Configuration conf, Map<String, String> tableParameters) {
    String value = conf.get(key);
    if (value == null && tableParameters != null) {
      value = tableParameters.get(key);
    }
    return Optional.fromNullable(value);
  }

  public static HiveBigQueryConfig from(Configuration conf, Map<String, String> tableParameters) {
    HiveBigQueryConfig config = new HiveBigQueryConfig();
    config.columnNameDelimiter =
        Optional.fromNullable(conf.get(serdeConstants.COLUMN_NAME_DELIMITER))
            .or(Optional.of(String.valueOf(SerDeUtils.COMMA)));
    config.traceId = Optional.of("Hive:" + HiveUtils.getHiveId(conf));
    config.proxyConfig = HiveBigQueryProxyConfig.from(conf);
    config.createDisposition =
        Optional.fromNullable(conf.get(CREATE_DISPOSITION_KEY))
            .transform(String::toUpperCase)
            .transform(JobInfo.CreateDisposition::valueOf);
    Optional<String> project = getAnyOption(PROJECT_KEY, conf, tableParameters);
    Optional<String> dataset = getAnyOption(DATASET_KEY, conf, tableParameters);
    Optional<String> table = getAnyOption(TABLE_KEY, conf, tableParameters);
    if (project.isPresent() && dataset.isPresent() && table.isPresent()) {
      config.tableId = TableId.of(project.get(), dataset.get(), table.get());
    }

    // Views
    config.viewsEnabled =
        Boolean.parseBoolean(getAnyOption(VIEWS_ENABLED_KEY, conf, tableParameters).or("false"));
    MaterializationConfiguration materializationConfiguration =
        MaterializationConfiguration.from(
            ImmutableMap.copyOf(conf.getPropsWithPrefix("")), new HashMap<>());
    config.materializationProject = materializationConfiguration.getMaterializationProject();
    config.materializationDataset = materializationConfiguration.getMaterializationDataset();
    config.materializationExpirationTimeInMinutes =
        materializationConfiguration.getMaterializationExpirationTimeInMinutes();

    // Reading options
    String readDataFormat =
        conf.get(HiveBigQueryConfig.READ_DATA_FORMAT_KEY, HiveBigQueryConfig.ARROW);
    if (readDataFormat.equals(HiveBigQueryConfig.ARROW)) {
      config.readDataFormat = DataFormat.ARROW;
    } else if (readDataFormat.equals(HiveBigQueryConfig.AVRO)) {
      config.readDataFormat = DataFormat.AVRO;
    } else {
      throw new RuntimeException("Invalid input read format type: " + readDataFormat);
    }
    config.createReadSessionTimeoutInSeconds =
        getAnyOption(READ_CREATE_SESSION_TIMEOUT_KEY, conf, tableParameters)
            .transform(Long::parseLong);
    config.maxParallelism =
        getAnyOption(READ_MAX_PARALLELISM, conf, tableParameters)
            .transform(Integer::parseInt)
            .orNull();
    config.preferredMinParallelism =
        getAnyOption(READ_PREFERRED_PARALLELISM, conf, tableParameters)
            .transform(Integer::parseInt)
            .orNull();

    // Credentials management
    config.credentialsKey = getAnyOption(CREDENTIALS_KEY_KEY, conf, tableParameters);
    config.credentialsFile =
        Optional.fromJavaUtil(
            firstPresent(
                getAnyOption(CREDENTIALS_FILE_KEY, conf, tableParameters).toJavaUtil(),
                Optional.fromNullable(conf.get(GCS_CONFIG_CREDENTIALS_FILE_PROPERTY))
                    .toJavaUtil()));
    config.accessToken = getAnyOption(ACCESS_TOKEN_KEY, conf, tableParameters);
    config.accessTokenProviderFQCN =
        getAnyOption(ACCESS_TOKEN_PROVIDER_FQCN_KEY, conf, tableParameters);

    // Partitioning and clustering
    config.partitionType =
        getAnyOption(TIME_PARTITION_TYPE_KEY, conf, tableParameters)
            .transform(TimePartitioning.Type::valueOf);
    config.partitionField = getAnyOption(TIME_PARTITION_FIELD_KEY, conf, tableParameters);
    config.partitionExpirationMs =
        getAnyOption(TIME_PARTITION_EXPIRATION_KEY, conf, tableParameters)
            .transform(Long::valueOf)
            .orNull();
    config.partitionRequireFilter =
        getAnyOption(TIME_PARTITION_REQUIRE_FILTER_KEY, conf, tableParameters)
            .transform(Boolean::valueOf);
    config.clusteredFields =
        getAnyOption(CLUSTERED_FIELDS_KEY, conf, tableParameters).transform(s -> s.split(","));
    return config;
  }

  private static Optional empty() {
    return Optional.absent();
  }

  @Override
  public TableId getTableId() {
    return tableId;
  }

  public String getColumnNameDelimiter() {
    return columnNameDelimiter.get();
  }

  @Override
  public java.util.Optional<CreateDisposition> getCreateDisposition() {
    return java.util.Optional.empty();
  }

  @Override
  public java.util.Optional<String> getPartitionField() {
    return partitionField.toJavaUtil();
  }

  // TODO: Enable other types of partitioning (e.g. Integer Range Partitioning)
  @Override
  public java.util.Optional<TimePartitioning.Type> getPartitionType() {
    return partitionType.toJavaUtil();
  }

  @Override
  public TimePartitioning.Type getPartitionTypeOrDefault() {
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
  public List<String> getDecimalTargetTypes() {
    return decimalTargetTypes;
  }

  @Override
  public List<SchemaUpdateOption> getLoadSchemaUpdateOptions() {
    return loadSchemaUpdateOptions;
  }

  @Override
  public boolean getEnableModeCheckForSchemaFields() {
    return enableModeCheckForSchemaFields;
  }

  @Override
  public java.util.Optional<String> getAccessTokenProviderFQCN() {
    return accessTokenProviderFQCN.toJavaUtil();
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
  public java.util.Optional<String> getBigQueryStorageGrpcEndpoint() {
    return bigQueryStorageGrpcEndpoint.toJavaUtil();
  }

  @Override
  public java.util.Optional<String> getBigQueryHttpEndpoint() {
    return bigQueryHttpEndpoint.toJavaUtil();
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

  @Override
  public java.util.Optional<Long> getCreateReadSessionTimeoutInSeconds() {
    return createReadSessionTimeoutInSeconds.toJavaUtil();
  }

  public OptionalInt getMaxParallelism() {
    return maxParallelism == null ? OptionalInt.empty() : OptionalInt.of(maxParallelism);
  }

  public OptionalInt getPreferredMinParallelism() {
    return preferredMinParallelism == null
        ? OptionalInt.empty()
        : OptionalInt.of(preferredMinParallelism);
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
        .setViewEnabledParamName(VIEWS_ENABLED_KEY)
        .setDefaultParallelism(1) // TODO: Make configurable?
        .setMaxParallelism(getMaxParallelism())
        .setPreferredMinParallelism(getPreferredMinParallelism())
        .setRequestEncodedBase(encodedCreateReadSessionRequest.toJavaUtil())
        .setBigQueryStorageGrpcEndpoint(bigQueryStorageGrpcEndpoint.toJavaUtil())
        .setBigQueryHttpEndpoint(bigQueryHttpEndpoint.toJavaUtil())
        .setBackgroundParsingThreads(numBackgroundThreadsPerStream)
        .setPushAllFilters(pushAllFilters)
        .setPrebufferReadRowsResponses(numPrebufferReadRowsResponses)
        .setStreamsPerPartition(numStreamsPerPartition)
        .setArrowCompressionCodec(arrowCompressionCodec)
        .setTraceId(traceId.toJavaUtil())
        .build();
  }
}
