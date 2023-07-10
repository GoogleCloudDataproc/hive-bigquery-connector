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
import com.google.cloud.hive.bigquery.connector.utils.JobUtils;
import com.google.cloud.hive.bigquery.connector.utils.JobUtils.CleanMessage;
import com.google.cloud.hive.bigquery.connector.utils.avro.AvroUtils;
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
import org.apache.hadoop.hive.metastore.DefaultHiveMetaHook;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.mapred.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class {@link BigQueryMetaHook} can be used to validate and perform different actions during the
 * creation and dropping of Hive tables, or during the execution of certain write operations.
 */
public class BigQueryMetaHook extends DefaultHiveMetaHook {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryMetaHook.class);

  public static final List<PrimitiveObjectInspector.PrimitiveCategory> SUPPORTED_HIVE_PRIMITIVES =
      ImmutableList.of(
          PrimitiveObjectInspector.PrimitiveCategory.BYTE, // Tiny Int
          PrimitiveObjectInspector.PrimitiveCategory.SHORT, // Small Int
          PrimitiveObjectInspector.PrimitiveCategory.INT, // Regular Int
          PrimitiveObjectInspector.PrimitiveCategory.LONG, // Big Int
          PrimitiveObjectInspector.PrimitiveCategory.FLOAT,
          PrimitiveObjectInspector.PrimitiveCategory.DOUBLE,
          PrimitiveObjectInspector.PrimitiveCategory.DATE,
          PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP,
          PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMPLOCALTZ,
          PrimitiveObjectInspector.PrimitiveCategory.BINARY,
          PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN,
          PrimitiveObjectInspector.PrimitiveCategory.CHAR,
          PrimitiveObjectInspector.PrimitiveCategory.VARCHAR,
          PrimitiveObjectInspector.PrimitiveCategory.STRING,
          PrimitiveObjectInspector.PrimitiveCategory.DECIMAL);

  Configuration conf;

  public BigQueryMetaHook(Configuration conf) {
    this.conf = conf;
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
      if (!SUPPORTED_HIVE_PRIMITIVES.contains(primitiveCategory)) {
        throw new MetaException("Unsupported Hive type: " + typeInfo.getTypeName());
      }
    } else {
      throw new MetaException("Unsupported Hive type: " + typeInfo.getTypeName());
    }
  }

  /** Validates that the given FieldSchemas are supported. */
  private void validateHiveTypes(List<FieldSchema> fieldSchemas) throws MetaException {
    for (FieldSchema schema : fieldSchemas) {
      TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(schema.getType());
      validateTypeInfo(typeInfo);
    }
  }

  /** Throws an exception if the table contains a column with the given name. */
  private void assertDoesNotContainColumn(Table hmsTable, String columnName) throws MetaException {
    List<FieldSchema> columns = hmsTable.getSd().getCols();
    for (FieldSchema column : columns) {
      if (column.getName().equalsIgnoreCase(columnName)) {
        throw new MetaException(
            String.format("%s already contains a column named `%s`", hmsTable, columnName));
      }
    }
  }

  private void createBigQueryTable(Table hmsTable, TableInfo bigQueryTableInfo) {
    Injector injector =
        Guice.createInjector(
            new BigQueryClientModule(),
            new HiveBigQueryConnectorModule(conf, hmsTable.getParameters()));
    HiveBigQueryConfig opts = injector.getInstance(HiveBigQueryConfig.class);
    BigQueryCredentialsSupplier credentialsSupplier =
        injector.getInstance(BigQueryCredentialsSupplier.class);
    HeaderProvider headerProvider = injector.getInstance(HeaderProvider.class);

    // TODO: We cannot use the BigQueryClient class here because it doesn't have a
    //  `create(TableInfo)` method. We could add it to that class eventually.
    BigQuery bigQueryService =
        BigQueryUtils.getBigQueryService(opts, headerProvider, credentialsSupplier);
    bigQueryService.create(bigQueryTableInfo);
  }

  private void configJobDetailsForIndirectWrite(
      HiveBigQueryConfig opts, JobDetails jobDetails, Schema bigQuerySchema, Injector injector)
      throws MetaException {
    // validate the temp GCS path to store the temporary Avro files
    validateTempGcsPath(opts.getTempGcsPath(), injector);
    // Convert BigQuery schema to Avro schema
    StructObjectInspector rowObjectInspector =
        BigQuerySerDe.getRowObjectInspector(jobDetails.getTableProperties());
    org.apache.avro.Schema avroSchema =
        AvroUtils.getAvroSchema(rowObjectInspector, bigQuerySchema.getFields());
    jobDetails.setAvroSchema(avroSchema);
  }

  /**
   * Performs required validations prior to creating the table
   *
   * @param table Represents hive table object
   * @throws MetaException if table metadata violates the constraints
   */
  @Override
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

    // Some environments rely on the "serialization.lib" table property instead of the
    // storage handler's getSerDeClass() method to pick the SerDe, so we set it here.
    table
        .getParameters()
        .put(
            serdeConstants.SERIALIZATION_LIB,
            "com.google.cloud.hive.bigquery.connector.BigQuerySerDe");
    table
        .getSd()
        .setInputFormat("com.google.cloud.hive.bigquery.connector.input.BigQueryInputFormat");
    table
        .getSd()
        .setOutputFormat("com.google.cloud.hive.bigquery.connector.output.BigQueryOutputFormat");

    Injector injector =
        Guice.createInjector(
            new BigQueryClientModule(),
            new HiveBigQueryConnectorModule(conf, table.getParameters()));
    BigQueryClient bqClient = injector.getInstance(BigQueryClient.class);
    HiveBigQueryConfig opts = injector.getInstance(HiveBigQueryConfig.class);
    if (MetaStoreUtils.isExternalTable(table)) {
      if (bqClient.tableExists(tableId)) {
        Map<String, String> basicStats = BigQueryUtils.getBasicStatistics(bqClient, tableId);
        basicStats.put(StatsSetupConst.COLUMN_STATS_ACCURATE, "{\"BASIC_STATS\":\"true\"}");
        table.getParameters().putAll(basicStats);
      } else {
        StatsSetupConst.setStatsStateForCreateTable(
            table.getParameters(), null, StatsSetupConst.FALSE);
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
        // This is an ingestion-time partition table, so we add the BigQuery
        // pseudo columns to the Hive MetaStore schema.
        assertDoesNotContainColumn(table, HiveBigQueryConfig.PARTITION_TIME_PSEUDO_COLUMN);
        table
            .getSd()
            .addToCols(
                new FieldSchema(
                    HiveBigQueryConfig.PARTITION_TIME_PSEUDO_COLUMN,
                    "timestamp with local time zone",
                    "Ingestion time pseudo column"));
        assertDoesNotContainColumn(table, HiveBigQueryConfig.PARTITION_DATE_PSEUDO_COLUMN);
        table
            .getSd()
            .addToCols(
                new FieldSchema(
                    HiveBigQueryConfig.PARTITION_DATE_PSEUDO_COLUMN,
                    "date",
                    "Ingestion time pseudo column"));
      }
      OptionalLong partitionExpirationMs = opts.getPartitionExpirationMs();
      if (partitionExpirationMs.isPresent()) {
        tpBuilder.setExpirationMs(partitionExpirationMs.getAsLong());
      }
      Optional<Boolean> partitionRequireFilter = opts.getPartitionRequireFilter();
      partitionRequireFilter.ifPresent(tpBuilder::setRequirePartitionFilter);
      tableDefBuilder.setTimePartitioning(tpBuilder.build());
    }

    StandardTableDefinition tableDefinition = tableDefBuilder.build();
    TableInfo bigQueryTableInfo =
        TableInfo.newBuilder(tableId, tableDefinition)
            .setDescription(table.getParameters().get("comment"))
            .build();
    createBigQueryTable(table, bigQueryTableInfo);

    String hmsDbTableName = HiveUtils.getDbTableName(table);
    LOG.info("Created BigQuery table {} for {}", tableId, hmsDbTableName);
    String tables = conf.get(HiveBigQueryConfig.CREATE_TABLES_KEY);
    tables =
        tables == null
            ? hmsDbTableName
            : tables + HiveBigQueryConfig.TABLE_NAME_SEPARATOR + hmsDbTableName;
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
          configJobDetailsForIndirectWrite(opts, jobDetails, tableSchema, injector);
        }
        Path queryTempOutputPath = JobUtils.getQueryTempOutputPath(conf, opts);
        jobDetails.setJobTempOutputPath(new Path(queryTempOutputPath, hmsDbTableName));
        JobDetails.writeJobDetailsFile(conf, jobDetailsFilePath, jobDetails);
      }
    } catch (IOException e) {
      LOG.warn("Can not update jobDetails for table {}", hmsDbTableName);
    }
  }

  @Override
  public void commitCreateTable(Table table) throws MetaException {
    // Do nothing yet
  }

  /** Called before insert query. */
  @Override
  public void preInsertTable(Table table, boolean overwrite) throws MetaException {
    Map<String, String> tableParameters = table.getParameters();
    Injector injector =
        Guice.createInjector(
            new BigQueryClientModule(), new HiveBigQueryConnectorModule(conf, tableParameters));
    BigQueryClient bqClient = injector.getInstance(BigQueryClient.class);
    HiveBigQueryConfig opts = injector.getInstance(HiveBigQueryConfig.class);

    // Load the job details file from HDFS
    JobDetails jobDetails;
    String hmsDbTableName = HiveUtils.getDbTableName(table);
    Path jobDetailsFilePath = JobUtils.getJobDetailsFilePath(conf, hmsDbTableName);
    try {
      jobDetails = JobDetails.readJobDetailsFile(conf, jobDetailsFilePath);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    TableId destTableId =
        BigQueryUtil.parseTableId(tableParameters.get(HiveBigQueryConfig.TABLE_KEY));
    TableInfo bqTableInfo = bqClient.getTable(jobDetails.getTableId());
    if (bqTableInfo == null) {
      throw new MetaException("BigQuery table does not exist: " + jobDetails.getTableId());
    }
    Schema bigQuerySchema = bqTableInfo.getDefinition().getSchema();

    if (jobDetails.getTableId() == null) {
      jobDetails.setTableId(destTableId);
    }
    jobDetails.setOverwrite(overwrite);
    jobDetails.setBigquerySchema(bigQuerySchema);

    if (opts.getWriteMethod().equals(HiveBigQueryConfig.WRITE_METHOD_DIRECT)) {
      // Special case: 'INSERT OVERWRITE' operation while using the 'direct'
      // write method. In this case, we will stream-write to a temporary table
      // and then finally overwrite the final destination table with the temporary
      // table's contents. This special case doesn't apply to the 'indirect'
      // write method, which doesn't need a temporary table -- instead that method
      // uses the 'WRITE_TRUNCATE' option available in the BigQuery Load Job API when
      // loading the Avro files into the BigQuery table (see more about that in the
      // `IndirectOutputCommitter` class).
      if (overwrite) {
        // Set the final destination table as the job's original table
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
                bigQuerySchema);
        // Set the temp table as the job's output table
        jobDetails.setTableId(tempTableInfo.getTableId());
        LOG.info("Insert overwrite temporary table {} ", tempTableInfo.getTableId());
      }
    } else {
      configJobDetailsForIndirectWrite(opts, jobDetails, bigQuerySchema, injector);
    }
    Path queryTempOutputPath = JobUtils.getQueryTempOutputPath(conf, opts);
    jobDetails.setJobTempOutputPath(new Path(queryTempOutputPath, hmsDbTableName));
    JobDetails.writeJobDetailsFile(conf, jobDetailsFilePath, jobDetails);
  }

  /**
   * This method is called automatically at the end of a successful job when using the "tez"
   * execution engine. This method is not called when using "mr" -- for that, see {@link
   * BigQueryOutputCommitter#commitJob(JobContext)}
   */
  @Override
  public void commitInsertTable(Table table, boolean overwrite) throws MetaException {
    try {
      JobDetails jobDetails = JobDetails.readJobDetailsFile(conf, HiveUtils.getDbTableName(table));
      BigQueryOutputCommitter.commit(conf, jobDetails);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      // deleteOnExit in case of other jobs using the same workdir
      JobUtils.cleanNotFail(
          () -> JobUtils.deleteQueryWorkDirOnExit(conf),
          CleanMessage.DELETE_QUERY_TEMPORARY_DIRECTORY);
    }
  }

  @Override
  public void rollbackInsertTable(Table table, boolean overwrite) throws MetaException {
    // Do nothing, should have been handled by committer
  }

  @Override
  public void commitDropTable(Table table, boolean deleteData) throws MetaException {
    if (!MetaStoreUtils.isExternalTable(table) && deleteData) {
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

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
    // Do nothing
  }

  @Override
  public void preDropTable(Table table) throws MetaException {
    // Do nothing
  }

  @Override
  public void rollbackDropTable(Table table) throws MetaException {
    // Do nothing
  }

  private TableId getTableId(Table table) throws MetaException {
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

  private void validateTempGcsPath(String tempGcsPath, Injector injector) throws MetaException {
    if (tempGcsPath == null || tempGcsPath.trim().equals("")) {
      throw new MetaException(
          String.format(
              "The '%s' property must be set when using the '%s' write method.",
              HiveBigQueryConfig.TEMP_GCS_PATH_KEY, HiveBigQueryConfig.WRITE_METHOD_INDIRECT));
    } else if (!JobUtils.hasGcsWriteAccess(
        injector.getInstance(BigQueryCredentialsSupplier.class), tempGcsPath)) {
      throw new MetaException(
          String.format(
              "Does not have write access to the following GCS path, or bucket does not exist: %s",
              tempGcsPath));
    }
  }
}
