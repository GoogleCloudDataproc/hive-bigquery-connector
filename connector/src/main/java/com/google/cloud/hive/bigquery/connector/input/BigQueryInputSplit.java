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

import static shaded.hivebqcon.com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.hive.bigquery.connector.Constants;
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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.io.HiveInputFormat.HiveInputSplit;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shaded.hivebqcon.com.google.cloud.bigquery.connector.common.*;
import shaded.hivebqcon.com.google.common.annotations.VisibleForTesting;
import shaded.hivebqcon.com.google.common.collect.ImmutableList;

public class BigQueryInputSplit extends HiveInputSplit implements Writable {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryInputSplit.class);

  private ReadRowsHelper readRowsHelper;
  private Path warehouseLocation;
  private String streamName;
  private List<String> columnNames;
  private BigQueryClientFactory bqClientFactory;
  private HiveBigQueryConfig config;
  private long hiveSplitLength;

  @VisibleForTesting
  public BigQueryInputSplit() {
    // Used by Hadoop serialization via reflection.
  }

  public BigQueryInputSplit(
      Path warehouseLocation,
      String streamName,
      List<String> columnNames,
      BigQueryClientFactory bqClientFactory,
      HiveBigQueryConfig config) {
    super();
    this.warehouseLocation = warehouseLocation;
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
    out.writeUTF(warehouseLocation.toString());
    out.writeUTF(streamName);
    out.writeLong(hiveSplitLength);
    byte[] columnNamesAsBytes = String.join(",", columnNames).getBytes(StandardCharsets.UTF_8);
    out.writeInt(columnNamesAsBytes.length);
    out.write(columnNamesAsBytes);
    writeObject(out, bqClientFactory);
    writeObject(out, config);
  }

  /** Hydrates the instance's attributes from the given sequence of bytes */
  public void readFields(DataInput in) throws IOException {
    warehouseLocation = new Path(in.readUTF());
    streamName = in.readUTF();
    hiveSplitLength = in.readLong();
    int length = in.readInt();
    byte[] columnNamesAsBytes = new byte[length];
    in.readFully(columnNamesAsBytes);
    columnNames = Arrays.asList(new String(columnNamesAsBytes, StandardCharsets.UTF_8).split(","));
    bqClientFactory = (BigQueryClientFactory) readObject(in);
    config = (HiveBigQueryConfig) readObject(in);
  }

  public void setHiveSplitLength(long hiveSplitLength) {
    this.hiveSplitLength = hiveSplitLength;
  }

  @Override
  public long getLength() {
    return this.hiveSplitLength;
  }

  @Override
  public String[] getLocations() throws IOException {
    // For data locality, irrelevant for bq.
    return new String[] {"*"};
  }

  @Override
  public String toString() {
    return String.format("warehouseLocation=%s, streamName=%s", warehouseLocation, streamName);
  }

  public String getStreamName() {
    return this.streamName;
  }

  @Override
  public Path getPath() {
    return warehouseLocation;
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public static InputSplit[] createSplitsFromBigQueryReadStreams(JobConf jobConf, int numSplits) {
    Injector injector =
        Guice.createInjector(new BigQueryClientModule(), new HiveBigQueryConnectorModule(jobConf));
    BigQueryClient bqClient = injector.getInstance(BigQueryClient.class);
    BigQueryClientFactory bqClientFactory = injector.getInstance(BigQueryClientFactory.class);
    HiveBigQueryConfig config = injector.getInstance(HiveBigQueryConfig.class);

    // Retrieve the table's column names
    String columnNameDelimiter = config.getColumnNameDelimiter();
    List<String> columnNames =
        new ArrayList<>(
            Arrays.asList(
                checkNotNull(jobConf.get(serdeConstants.LIST_COLUMNS)).split(columnNameDelimiter)));
    // Remove the virtual columns
    columnNames.removeAll(new HashSet<>(VirtualColumn.VIRTUAL_COLUMN_NAMES));

    Set<String> selectedFields;
    String engine = HiveConf.getVar(jobConf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE);
    if (engine.equals("mr")) {
      // To-Do: a workaround for HIVE-27115, remove when fix available.
      List<String> neededFields = getMRColumnProject(jobConf);
      selectedFields =
          neededFields.isEmpty() ? new HashSet<>(columnNames) : new HashSet<>(neededFields);
    } else {
      selectedFields =
          new HashSet<>(Arrays.asList(ColumnProjectionUtils.getReadColumnNames(jobConf)));
    }

    // Fix the BigQuery pseudo columns, if present, as Hive uses lowercase column names
    // whereas BigQuery expects the uppercase names.
    boolean found = selectedFields.remove(Constants.PARTITION_TIME_PSEUDO_COLUMN.toLowerCase());
    if (found) {
      selectedFields.add(Constants.PARTITION_TIME_PSEUDO_COLUMN);
    }
    found = selectedFields.remove(Constants.PARTITION_DATE_PSEUDO_COLUMN.toLowerCase());
    if (found) {
      selectedFields.add(Constants.PARTITION_DATE_PSEUDO_COLUMN);
    }

    // If a WHERE clause with filters is present, translate the filter values to
    // be compatible with BigQuery
    String serializedFilterExpr = jobConf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
    ExprNodeGenericFuncDesc filterExpr;
    Optional<String> filter = Optional.empty();
    if (serializedFilterExpr != null) {
      filterExpr = SerializationUtilities.deserializeExpression(serializedFilterExpr);
      ExprNodeGenericFuncDesc translatedFilterExpr =
          (ExprNodeGenericFuncDesc) BigQueryFilters.translateFilters(filterExpr, jobConf);
      if (translatedFilterExpr != null) {
        filter = Optional.of(translatedFilterExpr.getExprString());
      }
    }

    // Check that the BQ table in fact exists
    // TODO: Small optimization: Do the existence check in ReadSessionResponse.create() so we can
    //  save making this extra getTable() call to BigQuery. See:
    //  https://github.com/GoogleCloudDataproc/spark-bigquery-connector/issues/640
    TableInfo tableInfo = bqClient.getTable(config.getTableId());
    if (tableInfo == null) {
      throw new RuntimeException(
          "Table '" + BigQueryUtil.friendlyTableName(config.getTableId()) + "' not found");
    }

    LOG.info("Create readSession for {}", tableInfo);
    ReadSessionCreatorConfig readSessionCreatorConfig = config.toReadSessionCreatorConfig();
    ReadSessionCreator readSessionCreator =
        new ReadSessionCreator(readSessionCreatorConfig, bqClient, bqClientFactory);
    ReadSessionResponse readSessionResponse =
        readSessionCreator.create(
            config.getTableId(), ImmutableList.copyOf(selectedFields), filter);
    ReadSession readSession = readSessionResponse.getReadSession();

    Path warehouseLocation = new Path(jobConf.get("location"));
    // To-Do: replace when ReadStream has size estimation.
    long totalSize = readSession.getEstimatedTotalBytesScanned();
    int streamsCount = readSession.getStreamsCount();
    long hiveSplitSize = getHiveSplitLength(jobConf, totalSize, streamsCount, numSplits);
    return readSession.getStreamsList().stream()
        .map(
            readStream -> {
              BigQueryInputSplit split =
                  new BigQueryInputSplit(
                      warehouseLocation,
                      readStream.getName(),
                      columnNames,
                      bqClientFactory,
                      config);
              split.setHiveSplitLength(hiveSplitSize);
              return split;
            })
        .toArray(FileSplit[]::new);
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
              ImmutableList.of(request),
              config.toReadSessionCreatorConfig().toReadRowsHelperOptions());
    }
    return readRowsHelper;
  }

  /*
  Split size affects mapper task parallelism.
   */
  private static long getHiveSplitLength(
      JobConf jobConf, long totalSize, int streamCount, int requestGrpCount) {
    long avgStreamSize = streamCount == 0 ? totalSize : totalSize / streamCount;
    long hiveSplitSize = avgStreamSize;
    long minLengthPerGroup = jobConf.getLong("tez.grouping.min-size", 50 * 1024 * 1024L);
    long maxLengthPerGroup = jobConf.getLong("tez.grouping.max-size", 1024 * 1024 * 1024L);
    // To-Do: further performance investigation needed. initial testing shows disabling grouping
    // offers better performance.
    if (jobConf.getBoolean("bq.disable.tez.grouping", true) && avgStreamSize > minLengthPerGroup) {
      LOG.info("Set hiveSplitSize to try disable tez grouping.");
      long disableThreshold =
          Math.max(requestGrpCount, streamCount) * maxLengthPerGroup / streamCount;
      hiveSplitSize = Math.max(disableThreshold, avgStreamSize);
    }
    LOG.info(
        "BQ estimated totalSize={}, streamCount={}, avgStreamSize={}, set hiveSplitSize={}",
        totalSize,
        streamCount,
        avgStreamSize,
        hiveSplitSize);
    return hiveSplitSize;
  }

  // This is a workaround for HIVE-27115, used in MR mode.
  private static List<String> getMRColumnProject(JobConf jobConf) {
    String dir = jobConf.get("mapreduce.input.fileinputformat.inputdir");
    Path path = new Path(dir);
    try {
      MapWork mapWork = org.apache.hadoop.hive.ql.exec.Utilities.getMapWork(jobConf);
      if (mapWork == null
          || mapWork.getPathToAliases() == null
          || mapWork.getPathToAliases().isEmpty()) {
        return Collections.emptyList();
      }
      String alias = mapWork.getPathToAliases().get(path).get(0);
      TableScanDesc tableScanDesc = (TableScanDesc) mapWork.getAliasToWork().get(alias).getConf();
      return tableScanDesc.getNeededColumns();
    } catch (Exception e) {
      LOG.warn("Not able to find column project from plan for {}", dir);
      return Collections.emptyList();
    }
  }
}
