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
package com.google.cloud.hive.bigquery.connector.utils;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.hive.bigquery.connector.JobDetails;
import com.google.cloud.hive.bigquery.connector.output.WriterRegistry;
import com.google.common.truth.Truth;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

public class JobUtilsTest {

  @Test
  public void testExtractBucketNameFromGcsUri() {
    String bucket = JobUtils.extractBucketNameFromGcsUri("gs://abcd");
    assertEquals("abcd", bucket);
    bucket = JobUtils.extractBucketNameFromGcsUri("gs://abcd/path/to/file.csv");
    assertEquals("abcd", bucket);
  }

  @Test
  public void testTableIdPrefix() {
    TableId tableId = TableId.of("my:project", "mydataset", "mytable");
    String prefix = JobUtils.getTableIdPrefix(tableId);
    assertEquals("my__project_mydataset_mytable", prefix);
  }

  @Test
  public void testGetWorkDir() {
    Configuration conf = new Configuration();
    conf.set("hadoop.tmp.dir", "/tmp");
    Path path = JobUtils.getQueryWorkDir(conf);
    assertThat(path.toString()).matches("/tmp/hive-bq-custom-query-id-.*");
    conf.set("bq.work.dir.parent.path", "/my/workdir");
    path = JobUtils.getQueryWorkDir(conf);
    assertThat(path.toString()).matches("/my/workdir/hive-bq-custom-query-id-.*");
  }

  @Test
  public void testGetJobDetailsFilePath() {
    Configuration conf = new Configuration();
    conf.set("hadoop.tmp.dir", "/tmp");
    String hmsDbTable = "default.mytable";
    Path jobDetailsFilePath = JobUtils.getJobDetailsFilePath(conf, hmsDbTable);
    Truth.assertThat(jobDetailsFilePath.toString())
        .matches("/tmp/hive-bq-custom-query-id-.*/default\\.mytable/job-details\\.json");
  }

  @Test
  public void testGetTaskWriterOutputFile() {
    Configuration conf = new Configuration();
    conf.set("hadoop.tmp.dir", "/hadoop-tmp/");
    JobDetails jobDetails = new JobDetails();
    jobDetails.setTableProperties(new Properties());
    jobDetails.getTableProperties().put("name", "default.mytable");
    TableId tableId = TableId.of("myproject", "mydataset", "mytable");
    jobDetails.setTableId(tableId);
    String taskAttemptID = "abcd1234";
    String writerId = WriterRegistry.getWriterId();
    Path path = JobUtils.getTaskWriterOutputFile(conf, jobDetails, taskAttemptID, writerId, "jpeg");
    String pattern =
        "^/hadoop-tmp/hive-bq-custom-query-id-.*/default.mytable/myproject_mydataset_mytable_abcd1234_w\\d+\\.jpeg";
    assertThat(path.toString()).matches(pattern);
  }
}
