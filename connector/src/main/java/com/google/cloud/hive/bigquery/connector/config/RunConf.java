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
package com.google.cloud.hive.bigquery.connector.config;

import org.apache.hadoop.conf.Configuration;

/**
 * Class that gathers all the configuration keys and default values for the Hive-BigQuery connector.
 */
public class RunConf {

  public static final String WRITE_METHOD_DIRECT = "direct";
  public static final String WRITE_METHOD_INDIRECT = "indirect";
  public static final String ARROW = "arrow";
  public static final String AVRO = "avro";

  public enum Config {
    PROJECT("bq.project", null),
    DATASET("bq.dataset", null),
    TABLE("bq.table", null),
    WRITE_METHOD("bq.write.method", WRITE_METHOD_DIRECT),
    TEMP_GCS_PATH("bq.temp.gcs.path", null),
    WORK_DIR_PARENT_PATH("bq.work.dir.parent.path", null),
    WORK_DIR_NAME_PREFIX("bq.work.dir.name.prefix", "bq-hive-"),
    READ_DATA_FORMAT("bq.read.data.format", ARROW);

    private final String key;
    private final String defaultValue;

    Config(String key, String defaultValue) {
      this.key = key;
      this.defaultValue = defaultValue;
    }

    public String get(Configuration conf) {
      return conf.get(key, defaultValue);
    }

    public String getKey() {
      return key;
    }
  }
}
