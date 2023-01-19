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
package com.google.cloud.hive.bigquery.connector.input;

import static repackaged.by.hivebqconnector.com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.connector.common.*;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.hive.bigquery.connector.Constants;
import com.google.cloud.hive.bigquery.connector.PartitionSpec;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConnectorModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.io.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.io.HiveInputFormat.HiveInputSplit;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import repackaged.by.hivebqconnector.com.google.common.annotations.VisibleForTesting;
import repackaged.by.hivebqconnector.com.google.common.collect.ImmutableList;

public class BigQueryInputSplit extends HiveInputSplit implements Writable {

  private ReadRowsHelper readRowsHelper;
  private String location;
  private String streamName;
  private List<String> columnNames;
  private BigQueryClientFactory bqClientFactory;
  private HiveBigQueryConfig config;

  @VisibleForTesting
  public BigQueryInputSplit() {
    // Used by Hadoop serialization via reflection.
  }

  public BigQueryInputSplit(
      String location,
      String streamName,
      List<String> columnNames,
      BigQueryClientFactory bqClientFactory,
      HiveBigQueryConfig config) {
    super();
    this.location = location;
    this.streamName = streamName;
    this.columnNames = columnNames;
    this.bqClientFactory = bqClientFactory;
    this.config = config;
  }

  private void writeObject(DataOutput out, Object obj) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(obj);
      oos.close();
      byte[] rawBytes = baos.toByteArray();
      out.writeInt(rawBytes.length);
      out.write(rawBytes);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Object readObject(DataInput in) throws IOException {
    try {
      int length = in.readInt();
      byte[] rawBytes = new byte[length];
      in.readFully(rawBytes);
      ByteArrayInputStream bais = new ByteArrayInputStream(rawBytes);
      ObjectInputStream ois = new ObjectInputStream(bais);
      return ois.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }

  /** Serializes the instance's attributes to a sequence of bytes */
  public void write(DataOutput out) throws IOException {
    out.writeUTF(location);
    out.writeUTF(streamName);
    byte[] columnNamesAsBytes = String.join(",", columnNames).getBytes(StandardCharsets.UTF_8);
    out.writeInt(columnNamesAsBytes.length);
    out.write(columnNamesAsBytes);
    writeObject(out, bqClientFactory);
    writeObject(out, config);
  }

  /** Hydrates the instance's attributes from the given sequence of bytes */
  public void readFields(DataInput in) throws IOException {
    location = in.readUTF();
    streamName = in.readUTF();
    int length = in.readInt();
    byte[] columnNamesAsBytes = new byte[length];
    in.readFully(columnNamesAsBytes);
    columnNames = Arrays.asList(new String(columnNamesAsBytes, StandardCharsets.UTF_8).split(","));
    bqClientFactory = (BigQueryClientFactory) readObject(in);
    config = (HiveBigQueryConfig) readObject(in);
  }

  @Override
  public long getLength() {
    // FIXME: Supposedly should return the number of bytes in the input split.
    //  Might not be relevant in the context of BigQuery...
    return 1L;
  }

  @Override
  public String[] getLocations() throws IOException {
    // FIXME: Supposedly should return the list of hostnames where the input split is located.
    //  Might not be relevant in the context of BigQuery...
    return new String[0];
  }

  @Override
  public String toString() {
    return String.format("location=%s, streamName=%s", location, streamName);
  }

  public String getStreamName() {
    return this.streamName;
  }

  @Override
  public Path getPath() {
    return new Path(location);
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  /**
   * Figures out if we need to filter for a given partition. This is necessary when running a SELECT
   * query on a table that has a "PARTITION BY" clause. The specific partitions are retrieved from
   * the partition path(s) specified in the conf.
   */
  protected static ExprNodeGenericFuncDesc getPartitionFilter(JobConf jobConf, String inputDir) {
    String[] pathFragments = inputDir.split("/");
    String lastFragment = pathFragments[pathFragments.length - 1];
    ExprNodeGenericFuncDesc filter = null;
    if (lastFragment.contains("=")) {
      String[] nameValuePair = lastFragment.split("=");
      PartitionSpec partition =
          new PartitionSpec(
              nameValuePair[0],
              jobConf.get(hive_metastoreConstants.META_TABLE_PARTITION_COLUMN_TYPES),
              nameValuePair[1]);
      ExprNodeColumnDesc partitionColumnDesc =
          new ExprNodeColumnDesc(
              partition.getType(), partition.getName(), jobConf.get("name"), true);
      ExprNodeConstantDesc partitionValueDesc =
          new ExprNodeConstantDesc(partition.getType(), partition.getStaticValue());
      filter = new ExprNodeGenericFuncDesc();
      filter.setGenericUDF(new GenericUDFOPEqual());
      filter.setChildren(Arrays.asList(partitionColumnDesc, partitionValueDesc));
    }
    return filter;
  }

  /**
   * Combines the filter specified in the WHERE clause with a filter on the read partition, if any.
   */
  protected static Optional<String> getQueryFilter(
      JobConf jobConf, ExprNodeGenericFuncDesc partitionFilterDesc) {
    String serializedFilter = jobConf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
    ExprNodeGenericFuncDesc filterExpr = null;
    Optional<String> filter = Optional.empty();
    if (serializedFilter == null) {
      if (partitionFilterDesc != null) {
        filterExpr = partitionFilterDesc;
      }
    } else {
      if (partitionFilterDesc != null) {
        filterExpr = new ExprNodeGenericFuncDesc();
        filterExpr.setGenericUDF(new GenericUDFOPAnd());
        filterExpr.setChildren(
            Arrays.asList(
                partitionFilterDesc,
                SerializationUtilities.deserializeExpression(serializedFilter)));
      } else {
        filterExpr = SerializationUtilities.deserializeExpression(serializedFilter);
      }
    }

    // Translate the filter values to be compatible with BigQuery
    if (filterExpr != null) {
      ExprNodeGenericFuncDesc translatedFilterExpr =
          (ExprNodeGenericFuncDesc) BigQueryFilters.translateFilters(filterExpr);
      filter = Optional.of(translatedFilterExpr.getExprString());
    }

    return filter;
  }

  /** Returns all columns in the table. */
  protected static List<String> getAllColumnNames(JobConf jobConf, HiveBigQueryConfig config) {
    // Retrieve the table's column names
    String columnNameDelimiter = config.getColumnNameDelimiter();
    List<String> columnNames =
        new ArrayList<>(
            Arrays.asList(
                checkNotNull(jobConf.get(serdeConstants.LIST_COLUMNS)).split(columnNameDelimiter)));
    // Remove the virtual columns
    columnNames.removeAll(new HashSet<>(VirtualColumn.VIRTUAL_COLUMN_NAMES));
    return columnNames;
  }

  /** Figures out the table columns that the user wants the BigQuery Read API to return. */
  protected static ImmutableList<String> getSelectedColumns(
      JobConf jobConf, List<String> allColumnNames) {
    Set<String> selectedColumns;
    String engine = HiveConf.getVar(jobConf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE);
    if (engine.equals("mr")) {
      // Unfortunately the MR engine does not provide a reliable value for the
      // "hive.io.file.readcolumn.names" when multiple tables are read in the
      // same query. So we have to select all the columns (i.e. `SELECT *`).
      // This is unfortunately quite inefficient. Tez, however, does not have that issue.
      // See more info here: https://lists.apache.org/thread/g464zybq4g6c7p2h6nd9jmmznq472785
      // TODO: Investigate to see if we can come up with a workaround. Maybe try
      //  using the new MapRed API (org.apache.hadoop.mapreduce) instead of the old
      //  one (org.apache.hadoop.mapred)?
      selectedColumns = new HashSet<>(allColumnNames);
    } else {
      selectedColumns =
          new HashSet<>(Arrays.asList(ColumnProjectionUtils.getReadColumnNames(jobConf)));
    }

    // Fix the BigQuery pseudo columns, if present, as Hive uses lowercase column names
    // whereas BigQuery expects the uppercase names.
    boolean found = selectedColumns.remove(Constants.PARTITION_TIME_PSEUDO_COLUMN.toLowerCase());
    if (found) {
      selectedColumns.add(Constants.PARTITION_TIME_PSEUDO_COLUMN);
    }
    found = selectedColumns.remove(Constants.PARTITION_DATE_PSEUDO_COLUMN.toLowerCase());
    if (found) {
      selectedColumns.add(Constants.PARTITION_DATE_PSEUDO_COLUMN);
    }

    return ImmutableList.copyOf(selectedColumns);
  }

  public static InputSplit[] createSplitsFromBigQueryReadStreams(JobConf jobConf) {
    Injector injector =
        Guice.createInjector(new BigQueryClientModule(), new HiveBigQueryConnectorModule(jobConf));
    BigQueryClient bqClient = injector.getInstance(BigQueryClient.class);
    BigQueryClientFactory bqClientFactory = injector.getInstance(BigQueryClientFactory.class);
    HiveBigQueryConfig config = injector.getInstance(HiveBigQueryConfig.class);

    List<String> allColumnNames = getAllColumnNames(jobConf, config);
    ImmutableList<String> selectedColumns = getSelectedColumns(jobConf, allColumnNames);
    String[] inputDirs = jobConf.get("mapred.input.dir").split(",");

    // Check that the BQ table in fact exists
    // TODO: Small optimization: Do the existence check in ReadSessionResponse.create() so we can
    //  save making this extra getTable() call to BigQuery. See:
    //  https://github.com/GoogleCloudDataproc/spark-bigquery-connector/issues/640
    TableInfo tableInfo = bqClient.getTable(config.getTableId());
    if (tableInfo == null) {
      throw new RuntimeException(
          "Table '" + BigQueryUtil.friendlyTableName(config.getTableId()) + "' not found");
    }

    List<BigQueryInputSplit> inputSplits = new ArrayList<>();
    for (int i = 0; i < inputDirs.length; i++) {
      String inputDir = inputDirs[i];

      ExprNodeGenericFuncDesc partitionFilter = getPartitionFilter(jobConf, inputDir);
      Optional<String> queryFilter = getQueryFilter(jobConf, partitionFilter);

      // Create the BQ read session
      ReadSessionCreatorConfig readSessionCreatorConfig = config.toReadSessionCreatorConfig();
      ReadSessionCreator readSessionCreator =
          new ReadSessionCreator(readSessionCreatorConfig, bqClient, bqClientFactory);
      ReadSessionResponse readSessionResponse =
          readSessionCreator.create(config.getTableId(), selectedColumns, queryFilter);
      ReadSession readSession = readSessionResponse.getReadSession();

      // Assign a new InputSplit instance for each stream in the read session
      Stream<BigQueryInputSplit> stream =
          readSession.getStreamsList().stream()
              .map(
                  readStream ->
                      new BigQueryInputSplit(
                          inputDir, readStream.getName(), allColumnNames, bqClientFactory, config));
      inputSplits.addAll(stream.collect(Collectors.toList()));
    }

    return inputSplits.toArray(new BigQueryInputSplit[inputSplits.size()]);
  }

  /**
   * Creates and returns a ReadRowsHelper from the bigquery-connector-common library. The helper
   * takes care of reading data from the split's BQ stream.
   */
  public ReadRowsHelper getReadRowsHelper() {
    if (readRowsHelper == null) {
      ReadRowsRequest.Builder request =
          ReadRowsRequest.newBuilder().setReadStream(checkNotNull(getStreamName(), "name"));
      readRowsHelper =
          new ReadRowsHelper(
              bqClientFactory,
              request,
              config.toReadSessionCreatorConfig().toReadRowsHelperOptions());
    }
    return readRowsHelper;
  }
}
