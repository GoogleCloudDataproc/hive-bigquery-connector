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

import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import repackaged.by.hivebqconnector.com.google.common.base.Splitter;
import repackaged.by.hivebqconnector.com.google.common.collect.ImmutableList;

public class Constants {

  public static final String HADOOP_TMP_DIR_KEY = "hadoop.tmp.dir";
  public static final String HADOOP_COMMITTER_CLASS_KEY = "mapred.output.committer.class";
  public static final String HIVE_OUTPUT_TABLES_KEY = "hive.bq.output.tables";
  public static final String HIVE_CREATE_TABLES_KEY = "hive.bq.create.tables";
  public static final String TABLE_NAME_SEPARATOR = "|";
  public static final Splitter TABLE_NAME_SPLITTER = Splitter.on(TABLE_NAME_SEPARATOR);
  public static final String THIS_IS_AN_OUTPUT_JOB = "...this.is.an.output.job...";
  public static final String LOAD_FILE_EXTENSION = "avro";
  public static final String STREAM_FILE_EXTENSION = "stream";
  public static final String JOB_DETAILS_FILE = "job-details.json";

  // Pseudo columns in BigQuery for ingestion time partitioned tables
  public static final String PARTITION_TIME_PSEUDO_COLUMN = "_PARTITIONTIME";
  public static final String PARTITION_DATE_PSEUDO_COLUMN = "_PARTITIONDATE";

  // The maximum nesting depth of a BigQuery RECORD:
  public static final int MAX_BIGQUERY_NESTED_DEPTH = 15;

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
}
