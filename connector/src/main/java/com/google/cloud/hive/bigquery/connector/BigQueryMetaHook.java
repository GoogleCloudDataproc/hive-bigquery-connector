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
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientModule;
import com.google.cloud.bigquery.connector.common.BigQueryCredentialsSupplier;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConnectorModule;
import com.google.cloud.hive.bigquery.connector.output.BigQueryOutputCommitter;
import com.google.cloud.hive.bigquery.connector.utils.FileSystemUtils;
import com.google.cloud.hive.bigquery.connector.utils.bq.BigQuerySchemaConverter;
import com.google.cloud.hive.bigquery.connector.utils.bq.BigQueryUtils;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.DefaultHiveMetaHook;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.mapred.JobContext;
import repackaged.by.hivebqconnector.com.google.common.base.Strings;
import repackaged.by.hivebqconnector.com.google.common.collect.ImmutableList;

/**
 * Class {@link BigQueryMetaHook} can be used to validate and perform different actions during the
 * creation and dropping of Hive tables, or during the execution of certain write operations.
 */
public class BigQueryMetaHook extends DefaultHiveMetaHook {

  Configuration conf;
  TableInfo createTableInfo;

  public BigQueryMetaHook(Configuration conf) {
    this.conf = conf;
  }

  /** Validates that the given TypeInfo is supported. */
  private static void validateTypeInfo(TypeInfo typeInfo) throws MetaException {
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
  private static void validateHiveTypes(List<FieldSchema> columns) throws MetaException {
    for (FieldSchema column : columns) {
      TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(column.getType());
      validateTypeInfo(typeInfo);
    }
  }

  /** Throws an exception if the table contains a column with the given name. */
  private static void assertDoesNotContainColumn(
      String tableName, List<FieldSchema> columns, String columnName) throws MetaException {
    for (FieldSchema column : columns) {
      if (column.getName().equalsIgnoreCase(columnName)) {
        throw new MetaException(
            String.format(
                "Table `%s` already contains a column named `%s`", tableName, columnName));
      }
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
    JobDetails jobDetails =  JobDetails.getJobDetails(conf);

    if (jobDetails.isCTAS()) {
      // If it's a CTAS then we assume the BQ table has already been created
      // at the start of the job
      return;
    }
    //    table
    //        .getSd()
    //        .setInputFormat("com.google.cloud.hive.bigquery.connector.input.BigQueryInputFormat");
    //    table
    //        .getSd()
    //
    // .setOutputFormat("com.google.cloud.hive.bigquery.connector.output.BigQueryOutputFormat");
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
    createTableInfo = preCreateTable(conf, table.getSd().getCols(), table.getParameters());
  }

  public static TableInfo preCreateTable(
      Configuration conf, List<FieldSchema> columns, Map<String, String> tableProperties)
      throws MetaException {
    // Make sure the specified types are supported
    validateHiveTypes(columns);

    // Check all mandatory table properties
    ImmutableList<String> mandatory =
        ImmutableList.of(HiveBigQueryConfig.DATASET_KEY, HiveBigQueryConfig.TABLE_KEY);
    List<String> missingProperties = new ArrayList<>();
    for (String property : mandatory) {
      if (Strings.isNullOrEmpty(tableProperties.get(property))) {
        missingProperties.add(property);
      }
    }
    if (missingProperties.size() > 0) {
      throw new MetaException(
          "The following table property(ies) must be provided: "
              + String.join(", ", missingProperties));
    }

    // Some environments rely on the "serialization.lib" table property instead of the
    // storage handler's getSerDeClass() method to pick the SerDe, so we set it here.
    tableProperties.put(
        serdeConstants.SERIALIZATION_LIB, "com.google.cloud.hive.bigquery.connector.BigQuerySerDe");
    // Set the project property to the credentials project if not explicitly provided
    tableProperties.put(
        HiveBigQueryConfig.PROJECT_KEY,
        tableProperties.getOrDefault(
            HiveBigQueryConfig.PROJECT_KEY, BigQueryUtils.getDefaultProject()));

    if (MetaStoreUtils.isExternal(tableProperties)) {
      return null;
    } else { // If it's a managed table, generate the BigQuery schema
      Injector injector =
          Guice.createInjector(
              new BigQueryClientModule(), new HiveBigQueryConnectorModule(conf, tableProperties));
      BigQueryClient bqClient = injector.getInstance(BigQueryClient.class);
      HiveBigQueryConfig opts = injector.getInstance(HiveBigQueryConfig.class);
      if (bqClient.tableExists(opts.getTableId())) {
        throw new MetaException("BigQuery table already exists: " + opts.getTableId());
      }

      Schema tableSchema = BigQuerySchemaConverter.toBigQuerySchema(columns);

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
          assertDoesNotContainColumn(
              tableProperties.get("name"), columns, Constants.PARTITION_TIME_PSEUDO_COLUMN);
          columns.add(
              new FieldSchema(
                  Constants.PARTITION_TIME_PSEUDO_COLUMN,
                  "timestamp",
                  "Ingestion time pseudo column"));
          assertDoesNotContainColumn(
              tableProperties.get("name"), columns, Constants.PARTITION_DATE_PSEUDO_COLUMN);
          columns.add(
              new FieldSchema(
                  Constants.PARTITION_DATE_PSEUDO_COLUMN, "date", "Ingestion time pseudo column"));
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
      return TableInfo.newBuilder(opts.getTableId(), tableDefinition)
          .setDescription(tableProperties.get("comment"))
          .build();
    }
  }

  @Override
  public void commitCreateTable(Table table) throws MetaException {
    JobDetails jobDetails =  JobDetails.getJobDetails(conf);
    if (jobDetails.isCTAS()) {
      // If it's a CTAS then we assume the BQ table has already been created
      // at the start of the job
      return;
    }
    commitCreateTable(conf, table.getParameters(), createTableInfo);
  }

  public static void commitCreateTable(
      Configuration conf, Map<String, String> tableProperties, TableInfo tableInfo) {
    if (tableInfo == null) {
      // Nothing to do for external tables
      return;
    }
    // Create the managed table in BigQuery
    Injector injector =
        Guice.createInjector(
            new BigQueryClientModule(), new HiveBigQueryConnectorModule(conf, tableProperties));
    HiveBigQueryConfig opts = injector.getInstance(HiveBigQueryConfig.class);
    BigQueryCredentialsSupplier credentialsSupplier =
        injector.getInstance(BigQueryCredentialsSupplier.class);
    HeaderProvider headerProvider = injector.getInstance(HeaderProvider.class);

    // TODO: We cannot use the BigQueryClient class here because it doesn't have a
    //  `create(TableInfo)` method. We could add it to that class eventually.
    BigQuery bigQueryService =
        BigQueryUtils.getBigQueryService(opts, headerProvider, credentialsSupplier);
    bigQueryService.create(tableInfo);
  }

  /**
   * This method is called automatically at the end of a successful job when using the "tez"
   * execution engine. This method is not called when using "mr" -- for that, see {@link
   * BigQueryOutputCommitter#commitJob(JobContext)}
   */
  @Override
  public void commitInsertTable(Table table, boolean overwrite) throws MetaException {
    String engine = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE);
    if (engine.equals("tez")) {
      try {
        BigQueryOutputCommitter.commit(conf);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      throw new MetaException("Unexpected execution engine: " + engine);
    }
  }

  @Override
  public void rollbackInsertTable(Table table, boolean overwrite) throws MetaException {
    try {
      FileSystemUtils.deleteWorkDirOnExit(conf);
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

  public void preInsertTable(Table table, boolean overwrite) throws MetaException {
    // Do nothing
  }
}
