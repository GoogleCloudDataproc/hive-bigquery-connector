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
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConnectorModule;
import com.google.cloud.hive.bigquery.connector.output.BigQueryOutputCommitter;
import com.google.cloud.hive.bigquery.connector.output.indirect.IndirectUtils;
import com.google.cloud.hive.bigquery.connector.utils.FileSystemUtils;
import com.google.cloud.hive.bigquery.connector.utils.avro.AvroUtils;
import com.google.cloud.hive.bigquery.connector.utils.bq.BigQuerySchemaConverter;
import com.google.cloud.hive.bigquery.connector.utils.bq.BigQueryUtils;
import com.google.cloud.hive.bigquery.connector.utils.hive.HiveUtils;
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
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.mapred.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shaded.hivebqcon.com.google.cloud.bigquery.connector.common.BigQueryClient;
import shaded.hivebqcon.com.google.cloud.bigquery.connector.common.BigQueryClientModule;
import shaded.hivebqcon.com.google.cloud.bigquery.connector.common.BigQueryCredentialsSupplier;
import shaded.hivebqcon.com.google.common.base.Strings;
import shaded.hivebqcon.com.google.common.collect.ImmutableList;

/**
 * Class {@link BigQueryMetaHook} can be used to validate and perform different actions during the
 * creation and dropping of Hive tables, or during the execution of certain write operations.
 */
public class BigQueryMetaHook extends DefaultHiveMetaHook {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryMetaHook.class);

  Configuration conf;

  public BigQueryMetaHook(Configuration conf) {
    this.conf = conf;
  }

  private static String getDefaultProject() {
    return BigQueryOptions.getDefaultInstance().getService().getOptions().getProjectId();
  }

  private boolean isIndirectWrite() throws MetaException {
    String writeMethod =
        conf.get(HiveBigQueryConfig.WRITE_METHOD_KEY, HiveBigQueryConfig.WRITE_METHOD_DIRECT)
            .toLowerCase();
    if (writeMethod.equals(HiveBigQueryConfig.WRITE_METHOD_INDIRECT)) {
      return true;
    }
    if (writeMethod.equals(HiveBigQueryConfig.WRITE_METHOD_DIRECT)) {
      return false;
    }
    throw new MetaException("Invalid write method " + writeMethod);
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
      if (!Constants.SUPPORTED_HIVE_PRIMITIVES.contains(primitiveCategory)) {
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
      JobDetails jobDetails, Schema bigQuerySchema, Injector injector) throws MetaException {
    // Convert BigQuery schema to Avro schema
    StructObjectInspector rowObjectInspector =
        BigQuerySerDe.getRowObjectInspector(jobDetails.getTableProperties());
    org.apache.avro.Schema avroSchema =
        AvroUtils.getAvroSchema(rowObjectInspector, bigQuerySchema.getFields());
    jobDetails.setAvroSchema(avroSchema);
    // Set GCS path to store the temporary Avro files
    String tempGcsPath = conf.get(HiveBigQueryConfig.TEMP_GCS_PATH_KEY);
    jobDetails.setGcsTempPath(tempGcsPath);
    if (tempGcsPath == null || tempGcsPath.trim().equals("")) {
      throw new MetaException(
          String.format(
              "The '%s' property must be set when using the '%s' write method.",
              HiveBigQueryConfig.TEMP_GCS_PATH_KEY, HiveBigQueryConfig.WRITE_METHOD_INDIRECT));
    } else if (!IndirectUtils.hasGcsWriteAccess(
        injector.getInstance(BigQueryCredentialsSupplier.class), tempGcsPath)) {
      throw new MetaException(
          String.format(
              "Does not have write access to the following GCS path, or bucket does not exist: %s",
              tempGcsPath));
    }
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

    // Check all mandatory table properties
    ImmutableList<String> mandatory =
        ImmutableList.of(HiveBigQueryConfig.DATASET_KEY, HiveBigQueryConfig.TABLE_KEY);
    List<String> missingProperties = new ArrayList<>();
    for (String property : mandatory) {
      if (Strings.isNullOrEmpty(table.getParameters().get(property))) {
        missingProperties.add(property);
      }
    }
    if (missingProperties.size() > 0) {
      throw new MetaException(
          "The following table property(ies) must be provided: "
              + String.join(", ", missingProperties));
    }

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

    // Set the project property to the credentials project if not explicitly provided
    table
        .getParameters()
        .put(
            HiveBigQueryConfig.PROJECT_KEY,
            table
                .getParameters()
                .getOrDefault(HiveBigQueryConfig.PROJECT_KEY, getDefaultProject()));

    table
        .getSd()
        .setInputFormat("com.google.cloud.hive.bigquery.connector.input.BigQueryInputFormat");
    table
        .getSd()
        .setOutputFormat("com.google.cloud.hive.bigquery.connector.output.BigQueryOutputFormat");

    if (MetaStoreUtils.isExternalTable(table)) {
      // Unset stats
      StatsSetupConst.setStatsStateForCreateTable(
          table.getParameters(), null, StatsSetupConst.FALSE);
      return;
    }

    // If it's a managed table, generate the BigQuery schema
    Injector injector =
        Guice.createInjector(
            new BigQueryClientModule(),
            new HiveBigQueryConnectorModule(conf, table.getParameters()));
    BigQueryClient bqClient = injector.getInstance(BigQueryClient.class);
    HiveBigQueryConfig opts = injector.getInstance(HiveBigQueryConfig.class);
    if (bqClient.tableExists(opts.getTableId())) {
      throw new MetaException("BigQuery table already exists: " + opts.getTableId());
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
        assertDoesNotContainColumn(table, Constants.PARTITION_TIME_PSEUDO_COLUMN);
        table
            .getSd()
            .addToCols(
                new FieldSchema(
                    Constants.PARTITION_TIME_PSEUDO_COLUMN,
                    "timestamp",
                    "Ingestion time pseudo column"));
        assertDoesNotContainColumn(table, Constants.PARTITION_DATE_PSEUDO_COLUMN);
        table
            .getSd()
            .addToCols(
                new FieldSchema(
                    Constants.PARTITION_DATE_PSEUDO_COLUMN,
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
        TableInfo.newBuilder(opts.getTableId(), tableDefinition)
            .setDescription(table.getParameters().get("comment"))
            .build();
    createBigQueryTable(table, bigQueryTableInfo);

    String hmsDbTableName = HiveUtils.getDbTableName(table);
    LOG.info("Created BigQuery table {} for {}", opts.getTableId(), hmsDbTableName);
    String tables = conf.get(Constants.HIVE_CREATE_TABLES_KEY);
    tables =
        tables == null ? hmsDbTableName : tables + Constants.TABLE_NAME_SEPARATOR + hmsDbTableName;
    conf.set(Constants.HIVE_CREATE_TABLES_KEY, tables);

    try {
      Path jobDetailsFilePath = FileSystemUtils.getJobDetailsFilePath(conf, hmsDbTableName);
      // JobDetails file exists before table is created, likely a CTAS, we should update it
      if (jobDetailsFilePath.getFileSystem(conf).exists(jobDetailsFilePath)) {
        // Before we have a better way to handle CTAS in Tez, throw error
        if (HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE)
            .toLowerCase()
            .equals("tez")) {
          throw new MetaException("CTAS currently not supported in Tez mode for BigQuery table.");
        }
        JobDetails jobDetails = JobDetails.readJobDetailsFile(conf, hmsDbTableName);
        jobDetails.setBigquerySchema(tableSchema);

        if (isIndirectWrite()) {
          configJobDetailsForIndirectWrite(jobDetails, tableSchema, injector);
        }
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
  public void preInsertTable(Table table, boolean overwrite) throws MetaException {
    Map<String, String> tableParameters = table.getParameters();
    Injector injector =
        Guice.createInjector(
            new BigQueryClientModule(), new HiveBigQueryConnectorModule(conf, tableParameters));
    BigQueryClient bqClient = injector.getInstance(BigQueryClient.class);

    // Load the job details file from HDFS
    JobDetails jobDetails;
    String hmsDbTableName = HiveUtils.getDbTableName(table);
    Path jobDetailsFilePath = FileSystemUtils.getJobDetailsFilePath(conf, hmsDbTableName);
    try {
      jobDetails = JobDetails.readJobDetailsFile(conf, jobDetailsFilePath);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    // To-Do: jobdetails should already have those, add checks on jobDetails and merge properties
    jobDetails.setProject(tableParameters.get(HiveBigQueryConfig.PROJECT_KEY));
    jobDetails.setDataset(tableParameters.get(HiveBigQueryConfig.DATASET_KEY));
    String bqTableName = tableParameters.get(HiveBigQueryConfig.TABLE_KEY);
    jobDetails.setTable(bqTableName);
    jobDetails.setOverwrite(overwrite);

    // Retrieve the BigQuery schema of the final destination table
    TableInfo bqTableInfo = bqClient.getTable(jobDetails.getTableId());
    if (bqTableInfo == null) {
      throw new MetaException("BigQuery table does not exist: " + jobDetails.getTableId());
    }
    Schema bigQuerySchema = bqTableInfo.getDefinition().getSchema();
    jobDetails.setBigquerySchema(bigQuerySchema);

    boolean isDirectWrite = !isIndirectWrite();
    if (isDirectWrite) {
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
        jobDetails.setFinalTable(bqTableName);
        // Create a temporary table with the same schema
        // TODO: It'd be useful to add a description to the table explaining that it was
        //  created as a temporary table for a Hive query.
        TableInfo tempTableInfo =
            bqClient.createTempTable(
                TableId.of(
                    jobDetails.getProject(),
                    jobDetails.getDataset(),
                    bqTableName + "-" + HiveUtils.getHiveId(conf) + "-"),
                bigQuerySchema);
        // Set the temp table as the job's output table
        jobDetails.setTable(tempTableInfo.getTableId().getTable());
      }
    } else {
      // Convert BigQuery schema to Avro schema
      StructObjectInspector rowObjectInspector =
          BigQuerySerDe.getRowObjectInspector(jobDetails.getTableProperties());
      org.apache.avro.Schema avroSchema =
          AvroUtils.getAvroSchema(rowObjectInspector, bigQuerySchema.getFields());
      jobDetails.setAvroSchema(avroSchema);
      // Set GCS path to store the temporary Avro files
      String tempGcsPath = conf.get(HiveBigQueryConfig.TEMP_GCS_PATH_KEY);
      jobDetails.setGcsTempPath(tempGcsPath);
      if (tempGcsPath == null || tempGcsPath.trim().equals("")) {
        throw new MetaException(
            String.format(
                "The '%s' property must be set when using the '%s' write method.",
                HiveBigQueryConfig.TEMP_GCS_PATH_KEY, HiveBigQueryConfig.WRITE_METHOD_INDIRECT));
      } else if (!IndirectUtils.hasGcsWriteAccess(
          injector.getInstance(BigQueryCredentialsSupplier.class), tempGcsPath)) {
        throw new MetaException(
            String.format(
                "Cannot write to table '%s'. Does not have write access to the"
                    + " following GCS path, or bucket does not exist: %s",
                table.getTableName(), tempGcsPath));
      }
    }

    // Save the job details file so that we can retrieve all the information at
    // later stages of the job's execution
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
    }
  }

  @Override
  public void rollbackInsertTable(Table table, boolean overwrite) throws MetaException {
    try {
      String hmsDbTableName = HiveUtils.getDbTableName(table);
      FileSystemUtils.deleteWorkDirOnExit(conf, hmsDbTableName);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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
}
