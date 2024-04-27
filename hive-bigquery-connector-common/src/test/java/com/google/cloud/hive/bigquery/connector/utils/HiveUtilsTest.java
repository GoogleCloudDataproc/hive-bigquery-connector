/*
 * Copyright 2024 Google Inc. All Rights Reserved.
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
package com.google.cloud.hive.bigquery.connector.utils;

import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.hive.bigquery.connector.utils.hive.HiveUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class HiveUtilsTest {
  @Test
  public void testHiveQueryId() {
    Configuration conf = new Configuration();
    conf.set("hive.query.id", "abcd");
    assertEquals("hive-query-id-abcd", HiveUtils.getQueryId(conf));
  }

  @Test
  public void testPigQueryId() {
    Configuration conf = new Configuration();
    conf.set("pig.script.id", "abcd");
    conf.set("pig.job.submitted.timestamp", "999");
    assertEquals("pig-abcd-999", HiveUtils.getQueryId(conf));
  }

  @Test
  public void testMapreduceQueryId() {
    Configuration conf = new Configuration();
    conf.set("mapreduce.workflow.id", "abcd");
    assertEquals("mapreduce-abcd", HiveUtils.getQueryId(conf));
  }

  @Test
  public void testHCatQueryId() {
    Configuration conf = new Configuration();
    conf.set("mapreduce.lib.hcatoutput.id", "abcd");
    assertEquals("hcat-output-abcd", HiveUtils.getQueryId(conf));
  }
}
