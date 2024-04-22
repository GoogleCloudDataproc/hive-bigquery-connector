/*
 * Copyright 2024 Google Inc. All Rights Reserved.
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

import static com.google.common.truth.Truth.assertThat;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.JobInfo.CreateDisposition;
import com.google.cloud.bigquery.QueryJobConfiguration.Priority;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TimePartitioning.Type;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.threeten.bp.Duration;

public abstract class HiveBigQueryConfigTestBase {

  /** Make sure all members can be serialized. */
  @Test
  public void testSerializability() throws IOException {
    Configuration conf = new Configuration();
    conf.set("hive.query.id", "abcd");
    new ObjectOutputStream(new ByteArrayOutputStream())
        .writeObject(HiveBigQueryConfig.from(conf, new Properties()));
  }

  @Test
  public void testDefaults() {
    Configuration conf = new Configuration();
    conf.set("hive.query.id", "abcd");
    Injector injector =
        Guice.createInjector(new HiveBigQueryConnectorModule(conf, new Properties()));
    HiveBigQueryConfig opts = injector.getInstance(HiveBigQueryConfig.class);
    assertThat(opts.getWriteMethod()).isEqualTo("direct");
    assertThat(opts.getTableId()).isEqualTo(null);
    assertThat(opts.getTempGcsPath()).isEqualTo(null);
    assertThat(opts.getClusteredFields()).isEmpty();
    assertThat(opts.getPartitionField()).isEmpty();
    assertThat(opts.getPartitionRequireFilter()).isEmpty();
    assertThat(opts.getPartitionType()).isEmpty();
    assertThat(opts.getPartitionTypeOrDefault()).isEqualTo(Type.DAY);
    assertThat(opts.getPartitionExpirationMs()).isEmpty();
    assertThat(opts.getGpn()).isEmpty();
    assertThat(opts.getPreferredMinParallelism()).isEmpty();
    assertThat(opts.getMaxParallelism()).isEmpty();
    assertThat(opts.getReadDataFormat()).isEqualTo(DataFormat.ARROW);
    assertThat(opts.isWriteAtLeastOnce()).isFalse();
    assertThat(opts.getAccessToken()).isEmpty();
    assertThat(opts.getAccessTokenProviderConfig()).isEmpty();
    assertThat(opts.getAccessTokenProviderFQCN()).isEmpty();
    assertThat(opts.getBigQueryHttpEndpoint()).isEmpty();
    assertThat(opts.getBigQueryJobLabels()).isEmpty();
    assertThat(opts.getBigQueryJobTimeoutInMinutes()).isEqualTo(360);
    assertThat(opts.getBigQueryStorageGrpcEndpoint()).isEmpty();
    assertThat(opts.getCacheExpirationTimeInMinutes()).isEqualTo(15);
    assertThat(opts.getChannelPoolSize()).isEqualTo(1);
    assertThat(opts.getCreateDisposition()).isEmpty();
    assertThat(opts.getCreateReadSessionTimeoutInSeconds()).isEmpty();
    assertThat(opts.getCredentialsFile()).isEmpty();
    assertThat(opts.getCredentialsKey()).isEmpty();
    assertThat(opts.getDecimalTargetTypes()).isEqualTo(ImmutableList.of("NUMERIC", "BIGNUMERIC"));
    assertThat(opts.getFlowControlWindowBytes()).isEmpty();
    assertThat(opts.getImpersonationServiceAccount()).isEmpty();
    assertThat(opts.getImpersonationServiceAccountsForGroups().get()).isEmpty();
    assertThat(opts.getImpersonationServiceAccountsForUsers().get()).isEmpty();
    assertThat(opts.getKmsKeyName()).isEmpty();
    assertThat(opts.getLoadSchemaUpdateOptions()).isEmpty();
    assertThat(opts.getMaterializationProject()).isEmpty();
    assertThat(opts.getMaterializationDataset()).isEmpty();
    assertThat(opts.getQueryJobPriority()).isEqualTo(Priority.INTERACTIVE);
    assertThat(opts.getTraceId().get()).isEqualTo("Hive:hive-query-id-abcd");
    assertThat(opts.isUseAvroLogicalTypes()).isTrue();
    assertThat(opts.isViewsEnabled()).isFalse();
    assertThat(opts.useParentProjectForMetadataOperations()).isFalse();

    // Currently not implemented / not customizable
    assertThat(opts.getEnableModeCheckForSchemaFields()).isEqualTo(true);
    assertThat(opts.getBigQueryClientConnectTimeout()).isEqualTo(60000);
    assertThat(opts.getBigQueryClientReadTimeout()).isEqualTo(60000);
    assertThat(opts.getBigQueryClientRetrySettings())
        .isEqualTo(
            RetrySettings.newBuilder()
                .setMaxAttempts(10)
                .setTotalTimeout(Duration.ofMinutes(10))
                .setInitialRpcTimeout(Duration.ofSeconds(60))
                .setMaxRpcTimeout(Duration.ofMinutes(5))
                .setRpcTimeoutMultiplier(1.6)
                .setRetryDelayMultiplier(1.6)
                .setInitialRetryDelay(Duration.ofMillis(1250))
                .setMaxRetryDelay(Duration.ofSeconds(5))
                .build());
    assertThat(opts.getBigQueryProxyConfig().getProxyUri()).isEmpty();
    assertThat(opts.getBigQueryProxyConfig().getProxyPassword()).isEmpty();
    assertThat(opts.getBigQueryProxyConfig().getProxyPassword()).isEmpty();
    assertThat(opts.getPartitionRange()).isEmpty();
    assertThat(opts.getParentProjectId()).isEqualTo(null);

    // Dynamically set
    assertThat(opts.getLoggedInUserGroups()).isNotEmpty();
    assertThat(opts.getLoggedInUserName()).isNotEmpty();
  }

  @Test
  public void testHadoopConf() {
    Configuration conf = new Configuration();
    conf.set("bq.write.method", "indirect");
    conf.set("bq.temp.gcs.path", "gs://example/abcd");
    conf.set("bq.read.data.format", "avro");
    conf.set("bq.read.create.session.timeout", "999");
    conf.set("maxParallelism", "88");
    conf.set("preferredMinParallelism", "77");
    conf.set("bq.credentials.key", "KEYKEYKEY");
    conf.set("bq.credentials.file", "aaaa.json");
    conf.set("bq.access.token", "abcxyz");
    conf.set("bq.access.token.provider.fqcn", "YYYY");
    conf.set("bq.access.token.provider.config", "XXXX");
    conf.set("bq.impersonation.service.account.for.user.alice", "xxx@yyy.com");
    conf.set("bq.impersonation.service.account.for.user.bob", "eee@fff.com");
    conf.set("bq.impersonation.service.account.for.group.mygroup1", "ddd@ccc.com");
    conf.set("bq.impersonation.service.account.for.group.mygroup2", "ggg@mmm.com");
    conf.set("bq.impersonation.service.account", "aaa@bbb.com");
    conf.set("bq.create.disposition", "create_never");
    conf.set("bq.flow.control.window.bytes", "789");
    conf.set("bq.query.job.priority", "batch");
    conf.set("bq.write.at.least.once", "true");
    conf.set("GPN", "mygpn");
    conf.set("viewsEnabled", "true");
    conf.set("materializationProject", "myproject");
    conf.set("materializationDataset", "mydataset");
    conf.set("hive.query.id", "abcd");

    Injector injector =
        Guice.createInjector(new HiveBigQueryConnectorModule(conf, new Properties()));
    HiveBigQueryConfig opts = injector.getInstance(HiveBigQueryConfig.class);
    assertThat(opts.getWriteMethod()).isEqualTo("indirect");
    assertThat(opts.getTempGcsPath()).isEqualTo("gs://example/abcd");
    assertThat(opts.getReadDataFormat()).isEqualTo(DataFormat.AVRO);
    assertThat(opts.getCreateReadSessionTimeoutInSeconds()).isEqualTo(Optional.of(999L));
    assertThat(opts.getMaxParallelism()).isEqualTo(OptionalInt.of(88));
    assertThat(opts.getPreferredMinParallelism()).isEqualTo(OptionalInt.of(77));
    assertThat(opts.getCredentialsKey()).isEqualTo(Optional.of("KEYKEYKEY"));
    assertThat(opts.getCredentialsFile()).isEqualTo(Optional.of("aaaa.json"));
    assertThat(opts.getAccessToken()).isEqualTo(Optional.of("abcxyz"));
    assertThat(opts.getAccessTokenProviderFQCN()).isEqualTo(Optional.of("YYYY"));
    assertThat(opts.getAccessTokenProviderConfig()).isEqualTo(Optional.of("XXXX"));
    assertThat(opts.getImpersonationServiceAccountsForUsers())
        .isEqualTo(Optional.of(ImmutableMap.of("alice", "xxx@yyy.com", "bob", "eee@fff.com")));
    assertThat(opts.getImpersonationServiceAccountsForGroups())
        .isEqualTo(
            Optional.of(ImmutableMap.of("mygroup1", "ddd@ccc.com", "mygroup2", "ggg@mmm.com")));
    assertThat(opts.getImpersonationServiceAccount()).isEqualTo(Optional.of("aaa@bbb.com"));
    assertThat(opts.getCreateDisposition()).isEqualTo(Optional.of(CreateDisposition.CREATE_NEVER));
    assertThat(opts.getFlowControlWindowBytes()).isEqualTo(Optional.of(789));
    assertThat(opts.getQueryJobPriority()).isEqualTo(Priority.BATCH);
    assertThat(opts.isWriteAtLeastOnce()).isTrue();
    assertThat(opts.getGpn()).isEqualTo(Optional.of("mygpn"));
    assertThat(opts.isViewsEnabled()).isTrue();
    assertThat(opts.getMaterializationProject()).isEqualTo(Optional.of("myproject"));
    assertThat(opts.getMaterializationDataset()).isEqualTo(Optional.of("mydataset"));
    assertThat(opts.getTraceId()).isEqualTo(Optional.of("Hive:hive-query-id-abcd"));

    // Currently not implemented / not customizable
    assertThat(opts.getEnableModeCheckForSchemaFields()).isEqualTo(true);
    assertThat(opts.getBigQueryClientConnectTimeout()).isEqualTo(60000);
    assertThat(opts.getBigQueryClientReadTimeout()).isEqualTo(60000);
    assertThat(opts.getBigQueryClientRetrySettings())
        .isEqualTo(
            RetrySettings.newBuilder()
                .setMaxAttempts(10)
                .setTotalTimeout(Duration.ofMinutes(10))
                .setInitialRpcTimeout(Duration.ofSeconds(60))
                .setMaxRpcTimeout(Duration.ofMinutes(5))
                .setRpcTimeoutMultiplier(1.6)
                .setRetryDelayMultiplier(1.6)
                .setInitialRetryDelay(Duration.ofMillis(1250))
                .setMaxRetryDelay(Duration.ofSeconds(5))
                .build());
    assertThat(opts.getBigQueryProxyConfig().getProxyUri()).isEmpty();
    assertThat(opts.getBigQueryProxyConfig().getProxyPassword()).isEmpty();
    assertThat(opts.getBigQueryProxyConfig().getProxyPassword()).isEmpty();
    assertThat(opts.getPartitionRange()).isEmpty();
    assertThat(opts.getParentProjectId()).isEqualTo(null);
    assertThat(opts.getBigQueryHttpEndpoint()).isEmpty();
    assertThat(opts.getBigQueryJobLabels()).isEmpty();
    assertThat(opts.getBigQueryJobTimeoutInMinutes()).isEqualTo(360);
    assertThat(opts.getBigQueryStorageGrpcEndpoint()).isEmpty();
    assertThat(opts.getCacheExpirationTimeInMinutes()).isEqualTo(15);
    assertThat(opts.getChannelPoolSize()).isEqualTo(1);
    assertThat(opts.getDecimalTargetTypes()).isEqualTo(ImmutableList.of("NUMERIC", "BIGNUMERIC"));
    assertThat(opts.getKmsKeyName()).isEmpty();
    assertThat(opts.getLoadSchemaUpdateOptions()).isEmpty();
    assertThat(opts.isUseAvroLogicalTypes()).isTrue();
    assertThat(opts.useParentProjectForMetadataOperations()).isFalse();
  }

  @Test
  public void testTableProperties() {
    Configuration conf = new Configuration();
    conf.set("hive.query.id", "abcd");
    Properties props = new Properties();
    props.put("bq.table", "myotherproject.myotherdataset.myothertable");
    props.put("bq.time.partition.type", "month");
    props.put("bq.time.partition.field", "yyy");
    props.put("bq.time.partition.expiration.ms", "222");
    props.put("bq.time.partition.require.filter", "false");
    props.put("bq.clustered.fields", "d,e");

    Injector injector = Guice.createInjector(new HiveBigQueryConnectorModule(conf, props));
    HiveBigQueryConfig opts = injector.getInstance(HiveBigQueryConfig.class);
    assertThat(opts.getTableId())
        .isEqualTo(TableId.of("myotherproject", "myotherdataset", "myothertable"));
    assertThat(opts.getPartitionType()).isEqualTo(Optional.of(Type.MONTH));
    assertThat(opts.getPartitionTypeOrDefault()).isEqualTo(Type.MONTH);
    assertThat(opts.getPartitionField()).isEqualTo(Optional.of("yyy"));
    assertThat(opts.getPartitionExpirationMs()).isEqualTo(OptionalLong.of(222));
    assertThat(opts.getPartitionRequireFilter()).isEqualTo(Optional.of(false));
    assertThat(opts.getClusteredFields()).isEqualTo(Optional.of(Arrays.asList("d", "e")));
  }
}
