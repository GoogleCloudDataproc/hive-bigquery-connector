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

import static com.google.cloud.bigquery.connector.common.BigQueryConfigurationUtil.getAnyOptionsWithPrefix;
import static com.google.cloud.bigquery.connector.common.BigQueryConfigurationUtil.removePrefixFromMapKeys;
import static com.google.cloud.bigquery.connector.common.BigQueryUtil.firstPresent;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.JobInfo.CreateDisposition;
import com.google.cloud.bigquery.JobInfo.SchemaUpdateOption;
import com.google.cloud.bigquery.QueryJobConfiguration.Priority;
import com.google.cloud.bigquery.RangePartitioning.Range;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.connector.common.*;
import com.google.cloud.bigquery.storage.v1.ArrowSerializationOptions.CompressionCodec;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.hive.bigquery.connector.utils.hive.HiveUtils;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.threeten.bp.Duration;

/** Main config class to interact with the bigquery-common-connector. */
@SuppressWarnings("unchecked")
public class HiveBigQueryConfig
    implements BigQueryConfig, BigQueryClient.LoadDataOptions, Serializable {

  private static final long serialVersionUID = 1L;
  private static ImmutableMap<String, String> emptyMap = ImmutableMap.of();

  // Config keys
  public static final String WRITE_METHOD_KEY = "bq.write.method";
  public static final String TEMP_GCS_PATH_KEY = "bq.temp.gcs.path";
  public static final String WORK_DIR_PARENT_PATH_KEY = "bq.work.dir.parent.path";
  public static final String WORK_DIR_NAME_PREFIX_KEY = "bq.work.dir.name.prefix";
  public static final String WORK_DIR_NAME_PREFIX_DEFAULT = "hive-bq-";
  public static final String READ_DATA_FORMAT_KEY = "bq.read.data.format";
  public static final String READ_CREATE_SESSION_TIMEOUT_KEY = "bq.read.create.session.timeout";
  public static final String READ_MAX_PARALLELISM = "maxParallelism";
  public static final String READ_PREFERRED_PARALLELISM = "preferredMinParallelism";
  public static final String CREDENTIALS_KEY_KEY = "bq.credentials.key";
  public static final String CREDENTIALS_FILE_KEY = "bq.credentials.file";
  public static final String ACCESS_TOKEN_KEY = "bq.access.token";
  public static final String ACCESS_TOKEN_PROVIDER_FQCN_KEY = "bq.access.token.provider.fqcn";
  public static final String ACCESS_TOKEN_PROVIDER_CONFIG_KEY = "bq.access.token.provider.config";
  public static final String IMPERSONATE_FOR_USER_PREFIX =
      "bq.impersonation.service.account.for.user.";
  public static final String IMPERSONATE_FOR_GROUP_PREFIX =
      "bq.impersonation.service.account.for.group.";
  public static final String IMPERSONATE_SERVICE_ACCOUNT = "bq.impersonation.service.account";
  public static final String DESTINATION_TABLE_KMS_KEY_NAME = "bq.destination.table.kms.key.name";
  public static final String CREATE_DISPOSITION_KEY = "bq.create.disposition";
  public static final String VIEWS_ENABLED_KEY = "viewsEnabled";
  public static final String FAIL_ON_UNSUPPORTED_UDFS =
      "bq.fail.on.unsupported.udfs"; // Mainly used for testing
  public static final String OUTPUT_TABLES_KEY = "bq.output.tables";
  public static final String CREATE_TABLES_KEY = "bq.create.tables";
  public static final String HADOOP_COMMITTER_CLASS_KEY = "mapred.output.committer.class";
  public static final String FLOW_CONTROL_WINDOW_BYTES_KEY = "bq.flow.control.window.bytes";
  public static final String QUERY_JOB_PRIORITY_KEY = "bq.query.job.priority";
  public static final String WRITE_AT_LEAST_ONCE_KEY = "bq.write.at.least.once";

  // Table property keys
  public static final String TIME_PARTITION_TYPE_KEY = "bq.time.partition.type";
  public static final String TIME_PARTITION_FIELD_KEY = "bq.time.partition.field";
  public static final String TIME_PARTITION_EXPIRATION_KEY = "bq.time.partition.expiration.ms";
  public static final String TIME_PARTITION_REQUIRE_FILTER_KEY = "bq.time.partition.require.filter";
  public static final String CLUSTERED_FIELDS_KEY = "bq.clustered.fields";
  public static final String TABLE_KEY = "bq.table";

  // Pseudo columns in BigQuery for ingestion time partitioned tables
  public static final String PARTITION_TIME_PSEUDO_COLUMN = "_PARTITIONTIME";
  public static final String PARTITION_DATE_PSEUDO_COLUMN = "_PARTITIONDATE";

  // Other constants
  public static final int DEFAULT_CACHE_EXPIRATION_IN_MINUTES = 15;
  private static final int DEFAULT_BIGQUERY_CLIENT_CONNECT_TIMEOUT = 60 * 1000;
  private static final int DEFAULT_BIGQUERY_CLIENT_READ_TIMEOUT = 60 * 1000;
  private static final int DEFAULT_BIGQUERY_CLIENT_RETRIES = 10;
  static final String GCS_CONFIG_CREDENTIALS_FILE_PROPERTY =
      "google.cloud.auth.service.account.json.keyfile";
  static final long BIGQUERY_JOB_TIMEOUT_IN_MINUTES_DEFAULT = 6 * 60; // 6 hrs
  public static final String OUTPUT_TABLE_NAMES_SEPARATOR = "|";
  public static final Splitter OUTPUT_TABLE_NAMES_SPLITTER =
      Splitter.on(OUTPUT_TABLE_NAMES_SEPARATOR);
  public static final String THIS_IS_AN_OUTPUT_JOB = "...this.is.an.output.job...";
  public static final String LOAD_FILE_EXTENSION = "avro";
  public static final String STREAM_FILE_EXTENSION = "stream";
  public static final String JOB_DETAILS_FILE = "job-details.json";
  public static final String QUERY_ID = "bq.connector.query.id";
  public static final String HIVE_COLUMN_NAME_DELIMITER = ",";

  // For internal use only
  public static final String CONNECTOR_IN_TEST = "hive.bq.connector.in.test";
  public static final String FORCE_DROP_FAILURE = "hive.bq.connector.test.force.drop.failure";
  public static final String FORCED_DROP_FAILURE_ERROR_MESSAGE = "Forced table drop failure";
  public static final String FORCE_COMMIT_FAILURE = "hive.bq.connector.test.force.commit.failure";
  public static final String FORCED_COMMIT_FAILURE_ERROR_MESSAGE = "Forced commit failure";

  TableId tableId;
  Optional<String> traceId = empty();

  // Credentials management
  Optional<String> credentialsKey = empty();
  Optional<String> credentialsFile = empty();
  Optional<String> accessToken = empty();
  Optional<String> accessTokenProviderFQCN = empty();
  Optional<String> accessTokenProviderConfig = empty();
  String loggedInUserName;
  Set<String> loggedInUserGroups;
  Optional<String> impersonationServiceAccount;
  Optional<Map<String, String>> impersonationServiceAccountsForUsers;
  Optional<Map<String, String>> impersonationServiceAccountsForGroups;

  // KMS
  Optional<String> destinationTableKmsKeyName = empty();

  // Reading parameters
  DataFormat readDataFormat; // ARROW or AVRO
  Optional<Long> createReadSessionTimeoutInSeconds;
  public static final String ARROW = "arrow";
  public static final String AVRO = "avro";

  // Views
  boolean viewsEnabled;
  Optional<String> materializationProject;
  Optional<String> materializationDataset;
  int materializationExpirationTimeInMinutes;

  // Write method
  public static final String WRITE_METHOD_DIRECT = "direct";
  public static final String WRITE_METHOD_INDIRECT = "indirect";
  String writeMethod;

  /*
   * Options used for "indirect" write jobs.
   */
  // Indicates whether to interpret Avro logical types as the corresponding BigQuery data
  // type (for example, TIMESTAMP), instead of using the raw type (for example, LONG).
  boolean useAvroLogicalTypes = true;
  ImmutableList<String> decimalTargetTypes = ImmutableList.of("NUMERIC", "BIGNUMERIC");
  String tempGcsPath;

  // Partitioning and clustering
  Optional<String> partitionField = empty();
  Optional<TimePartitioning.Type> partitionType = empty();
  Long partitionExpirationMs = null;
  Optional<Boolean> partitionRequireFilter = empty();
  Optional<String[]> clusteredFields = empty();

  // Parallelism
  Integer maxParallelism = null;
  Integer preferredMinParallelism = null;

  // Misc
  private Optional<Integer> flowControlWindowBytes = empty();
  public static final Priority DEFAULT_JOB_PRIORITY = Priority.INTERACTIVE;
  private Priority queryJobPriority = DEFAULT_JOB_PRIORITY;
  public static final String GPN_ATTRIBUTION = "GPN";
  private Optional<String> gpn;
  boolean writeAtLeastOnce = false;

  // Options currently not implemented:
  HiveBigQueryProxyConfig proxyConfig;
  boolean enableModeCheckForSchemaFields = true;
  Optional<CreateDisposition> createDisposition = empty();
  ImmutableList<SchemaUpdateOption> loadSchemaUpdateOptions = ImmutableList.of();
  private ImmutableMap<String, String> bigQueryJobLabels = ImmutableMap.of();
  String parentProjectId;
  boolean useParentProjectForMetadataOperations;
  int maxReadRowsRetries = 3;
  private Optional<String> encodedCreateReadSessionRequest = empty();
  private Optional<String> bigQueryStorageGrpcEndpoint = empty();
  private Optional<String> bigQueryHttpEndpoint = empty();
  private int numBackgroundThreadsPerStream = 0;
  boolean pushAllFilters = true;
  private int numPrebufferReadRowsResponses = MIN_BUFFERED_RESPONSES_PER_STREAM;
  public static final int MIN_BUFFERED_RESPONSES_PER_STREAM = 1;
  private int numStreamsPerPartition = MIN_STREAMS_PER_PARTITION;
  public static final int MIN_STREAMS_PER_PARTITION = 1;
  private CompressionCodec arrowCompressionCodec = CompressionCodec.COMPRESSION_UNSPECIFIED;

  HiveBigQueryConfig() {
    // empty
  }

  public static Map<String, String> hadoopConfigAsMap(Configuration conf) {
    Iterator<Map.Entry<String, String>> iterator = conf.iterator();
    Map<String, String> configMap = new HashMap();
    while (iterator.hasNext()) {
      String name = iterator.next().getKey();
      String value = conf.get(name);
      configMap.put(name, value);
    }
    return configMap;
  }

  private static Optional<String> getOption(String key, Configuration conf) {
    return getOption(key, emptyMap, conf);
  }

  private static Optional<String> getOption(
      String key, Map<String, String> tableParameters, Configuration conf) {
    String value = tableParameters.get(key);
    if (value == null) {
      value = conf.get(key);
    }
    return Optional.fromNullable(value);
  }

  public static Map<String, String> convertPropertiesToMap(Properties properties) {
    Map<String, String> map = new HashMap<>();
    if (properties != null) {
      for (String key : properties.stringPropertyNames()) {
        String value = properties.getProperty(key);
        map.put(key, value);
      }
    }
    return map;
  }

  public static ImmutableMap<String, String> convertHadoopConfigurationToMap(Configuration conf) {
    HashMap<String, String> map = new HashMap<>();
    for (Map.Entry<String, String> entry : conf) {
      map.put(entry.getKey(), entry.getValue());
    }
    return ImmutableMap.copyOf(map);
  }

  public static HiveBigQueryConfig from(Configuration conf) {
    return from(conf, (Map<String, String>) null);
  }

  public static HiveBigQueryConfig from(Configuration conf, Properties tableProperties) {
    Map<String, String> map = convertPropertiesToMap(tableProperties);
    return from(conf, map);
  }

  public static HiveBigQueryConfig from(Configuration conf, Map<String, String> tableParameters) {
    ImmutableMap<String, String> confAsMap = convertHadoopConfigurationToMap(conf);
    if (tableParameters == null) {
      tableParameters = new HashMap<>();
    }
    HiveBigQueryConfig opts = new HiveBigQueryConfig();
    opts.traceId = Optional.of(getTraceId(conf));
    opts.proxyConfig = HiveBigQueryProxyConfig.from(conf);
    opts.createDisposition =
        getOption(CREATE_DISPOSITION_KEY, conf)
            .transform(String::toUpperCase)
            .transform(CreateDisposition::valueOf);
    opts.writeMethod = getWriteMethod(conf);
    opts.writeAtLeastOnce =
        Boolean.parseBoolean(getOption(WRITE_AT_LEAST_ONCE_KEY, conf).or("false"));
    opts.tempGcsPath = getOption(TEMP_GCS_PATH_KEY, conf).orNull();

    // Views
    opts.viewsEnabled = Boolean.parseBoolean(getOption(VIEWS_ENABLED_KEY, conf).or("false"));
    MaterializationConfiguration materializationConfiguration =
        MaterializationConfiguration.from(
            ImmutableMap.copyOf(hadoopConfigAsMap(conf)), new HashMap<>());
    opts.materializationProject = materializationConfiguration.getMaterializationProject();
    opts.materializationDataset = materializationConfiguration.getMaterializationDataset();
    opts.materializationExpirationTimeInMinutes =
        materializationConfiguration.getMaterializationExpirationTimeInMinutes();

    // Reading options
    String readDataFormat =
        conf.get(HiveBigQueryConfig.READ_DATA_FORMAT_KEY, HiveBigQueryConfig.ARROW).toLowerCase();
    if (readDataFormat.equals(HiveBigQueryConfig.ARROW)) {
      opts.readDataFormat = DataFormat.ARROW;
    } else if (readDataFormat.equals(HiveBigQueryConfig.AVRO)) {
      opts.readDataFormat = DataFormat.AVRO;
    } else {
      throw new RuntimeException("Invalid input read format type: " + readDataFormat);
    }
    opts.createReadSessionTimeoutInSeconds =
        getOption(READ_CREATE_SESSION_TIMEOUT_KEY, conf).transform(Long::parseLong);
    opts.maxParallelism =
        getOption(READ_MAX_PARALLELISM, conf).transform(Integer::parseInt).orNull();
    opts.preferredMinParallelism =
        getOption(READ_PREFERRED_PARALLELISM, conf).transform(Integer::parseInt).orNull();

    // Credentials management
    opts.credentialsKey = getOption(CREDENTIALS_KEY_KEY, conf);
    opts.credentialsFile =
        Optional.fromJavaUtil(
            firstPresent(
                getOption(CREDENTIALS_FILE_KEY, conf).toJavaUtil(),
                Optional.fromNullable(conf.get(GCS_CONFIG_CREDENTIALS_FILE_PROPERTY))
                    .toJavaUtil()));
    opts.accessToken = getOption(ACCESS_TOKEN_KEY, conf);
    opts.accessTokenProviderFQCN = getOption(ACCESS_TOKEN_PROVIDER_FQCN_KEY, conf);
    opts.accessTokenProviderConfig = getOption(ACCESS_TOKEN_PROVIDER_CONFIG_KEY, conf);
    try {
      UserGroupInformation ugiCurrentUser = UserGroupInformation.getCurrentUser();
      opts.loggedInUserName = ugiCurrentUser.getShortUserName();
      opts.loggedInUserGroups = Sets.newHashSet(ugiCurrentUser.getGroupNames());
    } catch (IOException e) {
      throw new BigQueryConnectorException(
          "Failed to get the UserGroupInformation current user", e);
    }
    opts.impersonationServiceAccount = getOption(IMPERSONATE_SERVICE_ACCOUNT, conf);
    opts.impersonationServiceAccountsForUsers =
        removePrefixFromMapKeys(
            getAnyOptionsWithPrefix(confAsMap, emptyMap, IMPERSONATE_FOR_USER_PREFIX),
            IMPERSONATE_FOR_USER_PREFIX);
    opts.impersonationServiceAccountsForGroups =
        removePrefixFromMapKeys(
            getAnyOptionsWithPrefix(confAsMap, emptyMap, IMPERSONATE_FOR_GROUP_PREFIX),
            IMPERSONATE_FOR_GROUP_PREFIX);

    // BigQuery Table ID
    Optional<String> bqTable = getOption(TABLE_KEY, tableParameters, conf);
    if (bqTable.isPresent()) {
      opts.tableId = BigQueryUtil.parseTableId(bqTable.get());
    }

    // KMS
    opts.destinationTableKmsKeyName =
        getOption(DESTINATION_TABLE_KMS_KEY_NAME, tableParameters, conf);

    // Partitioning and clustering
    opts.partitionType =
        getOption(TIME_PARTITION_TYPE_KEY, tableParameters, conf)
            .transform(String::toUpperCase)
            .transform(TimePartitioning.Type::valueOf);
    opts.partitionField = getOption(TIME_PARTITION_FIELD_KEY, tableParameters, conf);
    opts.partitionExpirationMs =
        getOption(TIME_PARTITION_EXPIRATION_KEY, tableParameters, conf)
            .transform(Long::valueOf)
            .orNull();
    opts.partitionRequireFilter =
        getOption(TIME_PARTITION_REQUIRE_FILTER_KEY, tableParameters, conf)
            .transform(Boolean::valueOf);
    opts.clusteredFields =
        getOption(CLUSTERED_FIELDS_KEY, tableParameters, conf).transform(s -> s.split(","));

    // Misc
    opts.flowControlWindowBytes =
        getOption(FLOW_CONTROL_WINDOW_BYTES_KEY, conf).transform(Integer::parseInt);
    opts.queryJobPriority =
        getOption(QUERY_JOB_PRIORITY_KEY, conf)
            .transform(String::toUpperCase)
            .transform(Priority::valueOf)
            .or(DEFAULT_JOB_PRIORITY);
    opts.gpn = getOption(GPN_ATTRIBUTION, conf);

    return opts;
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
    return createDisposition.toJavaUtil();
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
  public java.util.Optional<Range> getPartitionRange() {
    return java.util.Optional.empty();
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

  public void setLoadSchemaUpdateOptions(ImmutableList<SchemaUpdateOption> options) {
    loadSchemaUpdateOptions = options;
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
  public java.util.Optional<String> getAccessTokenProviderConfig() {
    return accessTokenProviderConfig.toJavaUtil();
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
  public String getLoggedInUserName() {
    return loggedInUserName;
  }

  @Override
  public Set<String> getLoggedInUserGroups() {
    return loggedInUserGroups;
  }

  @Override
  public java.util.Optional<Map<String, String>> getImpersonationServiceAccountsForUsers() {
    return impersonationServiceAccountsForUsers.toJavaUtil();
  }

  @Override
  public java.util.Optional<Map<String, String>> getImpersonationServiceAccountsForGroups() {
    return impersonationServiceAccountsForGroups.toJavaUtil();
  }

  @Override
  public java.util.Optional<String> getImpersonationServiceAccount() {
    return impersonationServiceAccount.toJavaUtil();
  }

  @Override
  public String getParentProjectId() {
    return parentProjectId;
  }

  @Override
  public java.util.Optional<String> getKmsKeyName() {
    return destinationTableKmsKeyName.toJavaUtil();
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
    // To-Do: make the setting configurable
    return getDefaultBigQueryClientRetrySettings();
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

  @Override
  public java.util.Optional<Integer> getFlowControlWindowBytes() {
    return flowControlWindowBytes.toJavaUtil();
  }

  @Override
  public Priority getQueryJobPriority() {
    return queryJobPriority;
  }

  @Override
  public long getBigQueryJobTimeoutInMinutes() {
    return BIGQUERY_JOB_TIMEOUT_IN_MINUTES_DEFAULT; // TODO: Make configurable
  }

  public boolean isWriteAtLeastOnce() {
    return writeAtLeastOnce;
  }

  @Override
  public int getChannelPoolSize() {
    return 1; // TODO: Make configurable
  }

  public OptionalInt getMaxParallelism() {
    return maxParallelism == null ? OptionalInt.empty() : OptionalInt.of(maxParallelism);
  }

  public OptionalInt getPreferredMinParallelism() {
    return preferredMinParallelism == null
        ? OptionalInt.empty()
        : OptionalInt.of(preferredMinParallelism);
  }

  public java.util.Optional<String> getTraceId() {
    return traceId.toJavaUtil();
  }

  public String getWriteMethod() {
    return writeMethod;
  }

  public String getTempGcsPath() {
    return tempGcsPath;
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

  public static String getWriteMethod(Configuration conf) {
    String writeMethod =
        getOption(HiveBigQueryConfig.WRITE_METHOD_KEY, conf).or(WRITE_METHOD_DIRECT).toLowerCase();
    if (!writeMethod.equals(WRITE_METHOD_DIRECT) && !writeMethod.equals(WRITE_METHOD_INDIRECT)) {
      throw new IllegalArgumentException("Invalid write method: " + writeMethod);
    }
    return writeMethod;
  }

  public static RetrySettings getDefaultBigQueryClientRetrySettings() {
    return RetrySettings.newBuilder()
        .setMaxAttempts(DEFAULT_BIGQUERY_CLIENT_RETRIES)
        .setTotalTimeout(Duration.ofMinutes(10))
        .setInitialRpcTimeout(Duration.ofSeconds(60))
        .setMaxRpcTimeout(Duration.ofMinutes(5))
        .setRpcTimeoutMultiplier(1.6)
        .setRetryDelayMultiplier(1.6)
        .setInitialRetryDelay(Duration.ofMillis(1250))
        .setMaxRetryDelay(Duration.ofSeconds(5))
        .build();
  }

  public static String getTraceId(Configuration conf) {
    return "Hive:" + HiveUtils.getQueryId(conf);
  }

  public java.util.Optional<String> getGpn() {
    return gpn.toJavaUtil();
  }
}
