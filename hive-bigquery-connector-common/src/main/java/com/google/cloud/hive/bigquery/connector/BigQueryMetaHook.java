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

import com.google.api.gax.rpc.HeaderProvider;
import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientModule;
import com.google.cloud.bigquery.connector.common.BigQueryCredentialsSupplier;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConnectorModule;
import com.google.cloud.hive.bigquery.connector.output.BigQueryOutputCommitter;
import com.google.cloud.hive.bigquery.connector.output.OutputCommitterUtils;
import com.google.cloud.hive.bigquery.connector.utils.JobUtils;
import com.google.cloud.hive.bigquery.connector.utils.bq.BigQuerySchemaConverter;
import com.google.cloud.hive.bigquery.connector.utils.bq.BigQueryUtils;
import com.google.cloud.hive.bigquery.connector.utils.hive.HiveUtils;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.mapred.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class {@link BigQueryMetaHook} can be used to validate and perform different actions during the
 * creation and dropping of Hive tables, or during the execution of certain write operations.
 */
public class BigQueryMetaHook {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryMetaHook.class);
  MetahookExtension extension;

  Configuration conf;

  public static final List<PrimitiveObjectInspector.PrimitiveCategory> basicTypes =
      Arrays.asList(
          PrimitiveCategory.BYTE, // Tiny Int
          PrimitiveCategory.SHORT, // Small Int
          PrimitiveCategory.INT, // Regular Int
          PrimitiveCategory.LONG, // Big Int
          PrimitiveCategory.FLOAT,
          PrimitiveCategory.DOUBLE,
          PrimitiveCategory.DATE,
          PrimitiveCategory.TIMESTAMP,
          PrimitiveCategory.BINARY,
          PrimitiveCategory.BOOLEAN,
          PrimitiveCategory.CHAR,
          PrimitiveCategory.VARCHAR,
          PrimitiveCategory.STRING,
          PrimitiveCategory.DECIMAL);

  public BigQueryMetaHook(Configuration conf) {
    this(conf, null);
  }

  public BigQueryMetaHook(Configuration conf, MetahookExtension extension) {
    this.conf = conf;
    this.extension = extension;
  }

  /** Validates that the given TypeInfo is supported. */
  private void validateTypeInfo(TypeInfo typeInfo) throws MetaException {
    if (typeInfo.getCategory() == Category.LIST) {
      validateTypeInfo(((ListTypeInfo) typeInfo).getListElementTypeInfo());
    } else if (typeInfo.getCategory() == Category.STRUCT) {
      ArrayList<TypeInfo> subTypeInfos = ((StructTypeInfo) typeInfo).getAllStructFieldTypeInfos();
      for (TypeInfo subTypeInfo : subTypeInfos) {
        validateTypeInfo(subTypeInfo);
      }
    } else if (typeInfo.getCategory() == Category.MAP) {
      TypeInfo mapKeyTypeInfo = ((MapTypeInfo) typeInfo).getMapKeyTypeInfo();
      validateTypeInfo(mapKeyTypeInfo);
      TypeInfo mapValueTypeInfo = ((MapTypeInfo) typeInfo).getMapValueTypeInfo();
      validateTypeInfo(mapValueTypeInfo);
    } else if (typeInfo.getCategory() == Category.PRIMITIVE) {
      PrimitiveCategory primitiveCategory = ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
      if (extension != null && !extension.getSupportedTypes().contains(primitiveCategory)) {
        throw new MetaException("Unsupported Hive type: " + typeInfo.getTypeName());
      }
    } else {
      throw new MetaException("Unsupported Hive type: " + typeInfo.getTypeName());
    }
  }

  /** Validates that the given FieldSchemas are supported. */
  protected void validateHiveTypes(List<FieldSchema> fieldSchemas) throws MetaException {
    for (FieldSchema schema : fieldSchemas) {
      TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(schema.getType());
      validateTypeInfo(typeInfo);
    }
  }

  /** Throws an exception if the table contains a column with the given name. */
  public static void assertDoesNotContainColumn(Table hmsTable, String columnName)
      throws MetaException {
    List<FieldSchema> columns = hmsTable.getSd().getCols();
    for (FieldSchema column : columns) {
      if (column.getName().equalsIgnoreCase(columnName)) {
        throw new MetaException(
            String.format("%s already contains a column named `%s`", hmsTable, columnName));
      }
    }
  }

  protected void createBigQueryTable(
      Injector injector,
      TableId tableId,
      StandardTableDefinition tableDefinition,
      HiveBigQueryConfig opts,
      Table hmsTable) {
    // TODO: We currently can't use the `BigQueryClient.createTable()` because it doesn't have a way
    // to
    //  pass a TableInfo. This forces us to duplicate some code below from the existing
    //  `BigQueryClient.createTable()`. One better long-term solution would be to add a
    //  `createTable(TableInfo)` method to BigQueryClient. See:
    // https://github.com/GoogleCloudDataproc/spark-bigquery-connector/issues/1213
    TableInfo.Builder bigQueryTableInfo =
        TableInfo.newBuilder(tableId, tableDefinition)
            .setDescription(hmsTable.getParameters().get("comment"));
    opts.getKmsKeyName()
        .ifPresent(
            keyName ->
                bigQueryTableInfo.setEncryptionConfiguration(
                    EncryptionConfiguration.newBuilder().setKmsKeyName(keyName).build()));
    BigQueryCredentialsSupplier credentialsSupplier =
        injector.getInstance(BigQueryCredentialsSupplier.class);
    HeaderProvider headerProvider = injector.getInstance(HeaderProvider.class);
    BigQuery bigQueryService =
        BigQueryUtils.getBigQueryService(opts, headerProvider, credentialsSupplier);
    bigQueryService.create(bigQueryTableInfo.build());
  }

  /**
   * Performs required validations prior to creating the table
   *
   * @param table Represents hive table object
   * @throws MetaException if table metadata violates the constraints
   */
  public void preCreateTable(Table table) throws MetaException {
    // Make sure the specified types are supported
    validateHiveTypes(table.getSd().getCols());

    TableId tableId = getTableId(table);
    table
        .getParameters()
        .put(
            HiveBigQueryConfig.TABLE_KEY,
            String.format(
                "%s.%s.%s", tableId.getProject(), tableId.getDataset(), tableId.getTable()));

    if (table.getPartitionKeysSize() > 0) {
      throw new MetaException(
          String.format(
              "Creating a partitioned table with the `PARTITIONED BY` clause is not supported. Use"
                  + " the `%s` table property instead.",
              HiveBigQueryConfig.TIME_PARTITION_FIELD_KEY));
    }

    if (table.getSd().getBucketColsSize() > 0) {
      throw new MetaException(
          String.format(
              "Creating a bucketed table with the `CLUSTERED BY` clause is not supported. Use the"
                  + " `%s` table property instead.",
              HiveBigQueryConfig.CLUSTERED_FIELDS_KEY));
    }

    if (!Strings.isNullOrEmpty(table.getSd().getLocation())) {
      throw new MetaException("Cannot create table in BigQuery with a `location` property.");
    }

    Injector injector =
        Guice.createInjector(
            new BigQueryClientModule(),
            new HiveBigQueryConnectorModule(conf, table.getParameters()));
    BigQueryClient bqClient = injector.getInstance(BigQueryClient.class);
    HiveBigQueryConfig opts = injector.getInstance(HiveBigQueryConfig.class);
    if (HiveUtils.isExternalTable(table)) {
      if (bqClient.tableExists(tableId)) {
        Map<String, String> basicStats = BigQueryUtils.getBasicStatistics(bqClient, tableId);
        basicStats.put(StatsSetupConst.COLUMN_STATS_ACCURATE, "{\"BASIC_STATS\":\"true\"}");
        table.getParameters().putAll(basicStats);
      } else {
        if (extension != null) {
          extension.setupStats(table);
        }
      }

      return;
    }

    // For managed table
    if (bqClient.tableExists(tableId)) {
      throw new MetaException("BigQuery table already exists: " + tableId);
    }

    Schema tableSchema = BigQuerySchemaConverter.toBigQuerySchema(table.getSd());

    StandardTableDefinition.Builder tableDefBuilder =
        StandardTableDefinition.newBuilder().setSchema(tableSchema);

    // Clustering
    Optional<ImmutableList<String>> clusteredFields = opts.getClusteredFields();
    if (clusteredFields.isPresent()) {
      Clustering clustering = Clustering.newBuilder().setFields(clusteredFields.get()).build();
      tableDefBuilder.setClustering(clustering);
    }

    // Time partitioning
    Optional<TimePartitioning.Type> partitionType = opts.getPartitionType();
    if (partitionType.isPresent()) {
      TimePartitioning.Builder tpBuilder = TimePartitioning.newBuilder(partitionType.get());
      Optional<String> partitionField = opts.getPartitionField();
      if (partitionField.isPresent()) {
        tpBuilder.setField(partitionField.get());
      } else {
        if (extension != null) {
          extension.setupIngestionTimePartitioning(table);
        }
      }
      OptionalLong partitionExpirationMs = opts.getPartitionExpirationMs();
      if (partitionExpirationMs.isPresent()) {
        tpBuilder.setExpirationMs(partitionExpirationMs.getAsLong());
      }
      Optional<Boolean> partitionRequireFilter = opts.getPartitionRequireFilter();
      partitionRequireFilter.ifPresent(tpBuilder::setRequirePartitionFilter);
      tableDefBuilder.setTimePartitioning(tpBuilder.build());
    }

    createBigQueryTable(injector, tableId, tableDefBuilder.build(), opts, table);

    String hmsDbTableName = HiveUtils.getDbTableName(table);
    LOG.info("Created BigQuery table {} for {}", tableId, hmsDbTableName);
    String tables = conf.get(HiveBigQueryConfig.CREATE_TABLES_KEY);
    tables =
        tables == null
            ? hmsDbTableName
            : tables + HiveBigQueryConfig.OUTPUT_TABLE_NAMES_SEPARATOR + hmsDbTableName;
    conf.set(HiveBigQueryConfig.CREATE_TABLES_KEY, tables);

    try {
      Path jobDetailsFilePath = JobUtils.getJobDetailsFilePath(conf, hmsDbTableName);
      // JobDetails file exists before table is created, likely a CTAS, we should update it
      if (jobDetailsFilePath.getFileSystem(conf).exists(jobDetailsFilePath)) {
        // Before we have a better way to handle CTAS in Tez, throw error
        if (HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE)
            .equalsIgnoreCase("tez")) {
          throw new MetaException("CTAS currently not supported in Tez mode for BigQuery table.");
        }
        JobDetails jobDetails = JobDetails.readJobDetailsFile(conf, hmsDbTableName);
        jobDetails.setBigquerySchema(tableSchema);
        if (opts.getWriteMethod().equals(HiveBigQueryConfig.WRITE_METHOD_INDIRECT)) {
          BigQueryStorageHandlerBase.configureJobDetailsForIndirectWrite(
              opts, jobDetails, injector.getInstance(BigQueryCredentialsSupplier.class));
        }
        jobDetails.writeFile(conf);
      }
    } catch (IOException e) {
      LOG.warn("Can not update jobDetails for table {}", hmsDbTableName);
    }
  }

  public void commitCreateTable(Table table) throws MetaException {
    // Do nothing yet
  }

  public static void makeOverwrite(Configuration conf, JobDetails jobDetails) {
    jobDetails.setOverwrite(true);
    Injector injector =
        Guice.createInjector(
            new BigQueryClientModule(),
            new HiveBigQueryConnectorModule(conf, jobDetails.getTableProperties()));
    BigQueryClient bqClient = injector.getInstance(BigQueryClient.class);
    if (jobDetails.getWriteMethod().equals(HiveBigQueryConfig.WRITE_METHOD_DIRECT)) {
      // Special case: 'INSERT OVERWRITE' operation while using the 'direct'
      // write method. In this case, we will stream-write to a temporary table
      // and then finally overwrite the final destination table with the temporary
      // table's contents. This special case doesn't apply to the 'indirect'
      // write method, which doesn't need a temporary table -- instead that method
      // uses the 'WRITE_TRUNCATE' option available in the BigQuery Load Job API when
      // loading the Avro files into the BigQuery table (see more about that in the
      // `IndirectOutputCommitter` class).

      // Set the final destination table as the job's original table
      TableId destTableId = jobDetails.getTableId();
      jobDetails.setFinalTableId(destTableId);
      // Create a temporary table with the same schema
      // TODO: It'd be useful to add a description to the table explaining that it was
      //  created as a temporary table for a Hive query.
      TableInfo tempTableInfo =
          bqClient.createTempTable(
              TableId.of(
                  destTableId.getProject(),
                  destTableId.getDataset(),
                  destTableId.getTable() + "-" + HiveUtils.getQueryId(conf) + "-"),
              jobDetails.getBigquerySchema());
      // Set the temp table as the job's output table
      jobDetails.setTableId(tempTableInfo.getTableId());
      LOG.info("Insert overwrite temporary table {} ", tempTableInfo.getTableId());
    }
  }

  /**
   * Called before insert query. It is only called by Hive 2 & 3. Spark, HCatalog, and Hive 1 do not
   * call it.
   */
  public void preInsertTable(String table, boolean overwrite) throws MetaException {
    // Load the job details file from HDFS
    JobDetails jobDetails;
    Path jobDetailsFilePath = JobUtils.getJobDetailsFilePath(conf, table);
    try {
      jobDetails = JobDetails.readJobDetailsFile(conf, jobDetailsFilePath);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (overwrite) {
      makeOverwrite(conf, jobDetails);
    }
    jobDetails.writeFile(conf);
  }

  /**
   * This method is called automatically at the end of a successful job when using the "tez"
   * execution engine in Hive 2 & 3. When using "mr", or "Tez" with Hive 1, Spark, or HCatalog, the
   * {@link BigQueryOutputCommitter#commitJob(JobContext)} method is called instead.
   */
  public void commitInsertTable(String table) throws MetaException {
    String engine = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).toLowerCase();
    if (engine.equals("tez")) {
      try {
        JobDetails jobDetails = JobDetails.readJobDetailsFile(conf, table);
        OutputCommitterUtils.commitJob(conf, jobDetails);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void rollbackInsertTable(Table table, boolean overwrite) throws MetaException {
    // Do nothing, should have been handled by committer
  }

  public void commitDropTable(Table table, boolean deleteData) throws MetaException {
    if (conf.getBoolean(HiveBigQueryConfig.CONNECTOR_IN_TEST, false)
        && conf.getBoolean(HiveBigQueryConfig.FORCE_DROP_FAILURE, false)) {
      // For integration testing only
      throw new RuntimeException(HiveBigQueryConfig.FORCED_DROP_FAILURE_ERROR_MESSAGE);
    }
    if (!HiveUtils.isExternalTable(table) && deleteData) {
      // This is a managed table, so let's delete the table in BigQuery
      Injector injector =
          Guice.createInjector(
              new BigQueryClientModule(),
              new HiveBigQueryConnectorModule(conf, table.getParameters()));
      BigQueryClient bqClient = injector.getInstance(BigQueryClient.class);
      HiveBigQueryConfig opts = injector.getInstance(HiveBigQueryConfig.class);
      bqClient.deleteTable(opts.getTableId());
    }
  }

  public void rollbackCreateTable(Table table) throws MetaException {
    // Do nothing
  }

  public void preDropTable(Table table) throws MetaException {
    // Do nothing
  }

  public void rollbackDropTable(Table table) throws MetaException {
    // Do nothing
  }

  protected TableId getTableId(Table table) throws MetaException {
    String bqTable = table.getParameters().get(HiveBigQueryConfig.TABLE_KEY);
    if (bqTable == null) {
      throw new MetaException("bq.table needs to be set in format of project.dataset.table.");
    }
    if (bqTable.split("\\.").length < 2) {
      throw new MetaException(
          "Need to provide bq.table in format of project.dataset.table, if project is missing will"
              + " use default project.");
    }
    return BigQueryUtil.parseTableId(bqTable);
  }
}
