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

import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.hive.bigquery.connector.config.RunConf;
import com.google.cloud.hive.bigquery.connector.output.direct.DirectUtils;
import com.google.cloud.hive.bigquery.connector.output.fileload.FileLoadUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.junit.jupiter.api.Test;

public class UnitTests {

  @Test
  public void testGcsTempDir() {
    Configuration conf = new Configuration();
    conf.set(RunConf.Config.WORK_DIR_NAME_PREFIX.getKey(), "xyz");
    conf.set(HiveConf.ConfVars.HIVEQUERYID.varname, "query123456");
    Path gcsTempDir = FileLoadUtils.getGcsTempDir(conf, "gs://example/abcd");
    assertEquals("gs://example/abcd/xyzquery123456", gcsTempDir.toString());
  }

  @Test
  public void testExtractBucketNameFromGcsUri() {
    String bucket = FileLoadUtils.extractBucketNameFromGcsUri("gs://abcd");
    assertEquals("abcd", bucket);
    bucket = FileLoadUtils.extractBucketNameFromGcsUri("gs://abcd/path/to/file.csv");
    assertEquals("abcd", bucket);
  }

  @Test
  public void testGetTaskTempAvroFileNamePrefix() {
    TableId tableId = TableId.of("myproject", "mydataset", "mytable");
    String prefix = FileLoadUtils.getTaskTempAvroFileNamePrefix(tableId);
    assertEquals("myproject_mydataset_mytable", prefix);
  }

  @Test
  public void testGetTaskAvroTempFile() {
    Configuration conf = new Configuration();
    conf.set("hive.query.id", "query123");
    TableId tableId = TableId.of("myproject", "mydataset", "mytable");
    TaskAttemptID taskAttemptID = new TaskAttemptID();
    Path path =
        FileLoadUtils.getTaskAvroTempFile(conf, tableId, "gs://example/mypath", taskAttemptID);
    assertEquals(
        "gs://example/mypath/bq-hive-query123/myproject_mydataset_mytable_task__0000_r_000000.avro",
        path.toString());
  }

  @Test
  public void testGetTaskTempStreamFileNamePrefix() {
    TableId tableId = TableId.of("myproject", "mydataset", "mytable");
    String prefix = DirectUtils.getTaskTempStreamFileNamePrefix(tableId);
    assertEquals("myproject_mydataset_mytable", prefix);
  }

  @Test
  public void testGetTaskTempStreamFile() {
    Configuration conf = new Configuration();
    conf.set("hive.query.id", "query123");
    conf.set("bq.work.dir.parent.path", "/my/workdir");
    TableId tableId = TableId.of("myproject", "mydataset", "mytable");
    TaskAttemptID taskAttemptID = new TaskAttemptID();
    Path path = DirectUtils.getTaskTempStreamFile(conf, tableId, taskAttemptID);
    assertEquals(
        "/my/workdir/bq-hive-query123/myproject_mydataset_mytable_task__0000_r_000000.stream",
        path.toString());
  }
}
