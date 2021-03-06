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
import repackaged.by.hivebqconnector.com.google.common.collect.ImmutableList;

public class Constants {

  public static final String HADOOP_TMP_DIR_KEY = "hadoop.tmp.dir";
  public static final String HADOOP_COMMITTER_CLASS_KEY = "mapred.output.committer.class";
  public static final String THIS_IS_AN_OUTPUT_JOB = "...this.is.an.output.job...";
  public static final String LOAD_FILE_EXTENSION = "avro";
  public static final String STREAM_FILE_EXTENSION = "stream";
  public static final String INFO_FILE = "info.json";

  // The maximum nesting depth of a BigQuery RECORD:
  public static final int MAX_BIGQUERY_NESTED_DEPTH = 15;
  public static final String MAPTYPE_ERROR_MESSAGE = "MapType is unsupported.";

  public static final List<PrimitiveObjectInspector.PrimitiveCategory> SUPPORTED_HIVE_PRIMITIVES =
      ImmutableList.of(
          PrimitiveObjectInspector.PrimitiveCategory.LONG,
          PrimitiveObjectInspector.PrimitiveCategory.DOUBLE,
          PrimitiveObjectInspector.PrimitiveCategory.DATE,
          PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP,
          PrimitiveObjectInspector.PrimitiveCategory.BINARY,
          PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN,
          PrimitiveObjectInspector.PrimitiveCategory.STRING,
          PrimitiveObjectInspector.PrimitiveCategory.DECIMAL);
}
