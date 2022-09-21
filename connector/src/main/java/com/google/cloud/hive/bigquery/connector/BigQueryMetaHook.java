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

import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.connector.common.BigQueryClientModule;
import com.google.cloud.bigquery.connector.common.BigQueryCredentialsSupplier;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConnectorModule;
import com.google.cloud.hive.bigquery.connector.output.BigQueryOutputCommitter;
import com.google.cloud.hive.bigquery.connector.output.indirect.IndirectUtils;
import com.google.cloud.hive.bigquery.connector.utils.HiveUtils;
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

  public BigQueryMetaHook(Configuration conf) {
    this.conf = conf;
  }

  private static String getDefaultProject() {
    return BigQueryOptions.getDefaultInstance().getService().getOptions().getProjectId();
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

    // Check compatibility with BigQuery features
    // TODO: accept DATE column 1 level partitioning
    if (table.getPartitionKeysSize() > 0) {
      throw new MetaException("Creation of Partition table is not supported.");
    }

    if (table.getSd().getBucketColsSize() > 0) {
      throw new MetaException("Creation of bucketed table is not supported");
    }

    if (!Strings.isNullOrEmpty(table.getSd().getLocation())) {
      throw new MetaException("Cannot create table in BigQuery with Location property.");
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
  }

  /** Called before data is written to a table. */
  @Override
  public void preInsertTable(Table table, boolean overwrite) throws MetaException {
    // Load the job info file from HDFS
    JobInfo jobInfo;
    try {
      jobInfo = JobInfo.readInfoFile(conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Map<String, String> tableParameters = table.getParameters();
    jobInfo.setProject(tableParameters.get(HiveBigQueryConfig.PROJECT_KEY));
    jobInfo.setDataset(tableParameters.get(HiveBigQueryConfig.DATASET_KEY));
    String tableName = tableParameters.get(HiveBigQueryConfig.TABLE_KEY);
    jobInfo.setTable(tableName);
    jobInfo.setOverwrite(overwrite);

    // Note: Unfortunately the table properties do not contain constraints like
    // "NOT NULL", so the inferred avro & proto schema assume that all columns are
    // optional (e.g. UNION["null", "long"]). So if these constraints are necessary
    // for the user, then the user should provide an explicit avro schema at table
    // creation time.
    // See: https://lists.apache.org/thread/mjm4yznf87xzbk7xywf2gvmnp3l1dm5d

    String writeMethod =
        conf.get(HiveBigQueryConfig.WRITE_METHOD_KEY, HiveBigQueryConfig.WRITE_METHOD_DIRECT);
    Injector injector =
        Guice.createInjector(
            new BigQueryClientModule(), new HiveBigQueryConnectorModule(conf, tableParameters));
    if (writeMethod.equals(HiveBigQueryConfig.WRITE_METHOD_DIRECT)) {
      // Get an instance of the BigQuery client
      BigQueryClient bqClient = injector.getInstance(BigQueryClient.class);

      // Retrieve the BigQuery schema of the final destination table
      Schema bigQuerySchema = bqClient.getTable(jobInfo.getTableId()).getDefinition().getSchema();

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
        jobInfo.setFinalTable(tableName);
        // Create a temporary table with the same schema
        // TODO: It'd be useful to add a description to the table explaining that it was
        //  created as a temporary table for a Hive query.
        TableInfo tableInfo =
            bqClient.createTempTable(
                TableId.of(
                    jobInfo.getProject(),
                    jobInfo.getDataset(),
                    tableName + "-" + HiveUtils.getHiveId(conf) + "-"),
                bigQuerySchema);
        // Set the temp table as the job's output table
        jobInfo.setTable(tableInfo.getTableId().getTable());
      }
    } else if (writeMethod.equals(HiveBigQueryConfig.WRITE_METHOD_INDIRECT)) {
      String temporaryGcsPath = conf.get(HiveBigQueryConfig.TEMP_GCS_PATH_KEY);
      jobInfo.setGcsTempPath(temporaryGcsPath);
      if (temporaryGcsPath == null || temporaryGcsPath.trim().equals("")) {
        throw new MetaException(
            String.format(
                "The '%s' property must be set when using the '%s' write method.",
                HiveBigQueryConfig.TEMP_GCS_PATH_KEY, HiveBigQueryConfig.WRITE_METHOD_INDIRECT));
      } else if (!IndirectUtils.hasGcsWriteAccess(injector.getInstance(BigQueryCredentialsSupplier.class), temporaryGcsPath)) {
        throw new MetaException(
            String.format(
                "Cannot write to table '%s'. The service account does not have IAM permissions to write to the"
                    + " following GCS path, or bucket does not exist: %s",
                table.getTableName(), temporaryGcsPath));
      }
    } else {
      throw new MetaException("Invalid write method: " + writeMethod);
    }

    // Save the info file so that we can retrieve all the information at later
    // stages of the job's execution
    JobInfo.writeInfoFile(conf, jobInfo);
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
  public void rollbackInsertTable(Table table, boolean overwrite) throws MetaException {}

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
    // Do nothing
  }

  @Override
  public void commitCreateTable(Table table) throws MetaException {
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

  @Override
  public void commitDropTable(Table table, boolean b) throws MetaException {
    // Do nothing
  }
}
