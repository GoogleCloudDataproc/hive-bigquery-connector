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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.text.MatchesPattern.matchesPattern;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.hive.bigquery.connector.JobDetails;
import com.google.cloud.hive.bigquery.connector.output.WriterRegistry;
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
    conf.set("hive.query.id", "query123");
    conf.set("hadoop.tmp.dir", "/tmp");
    Path path = JobUtils.getQueryWorkDir(conf);
    assertEquals("/tmp/bq-hive-hive-query-id-query123", path.toString());
    conf.set("bq.work.dir.parent.path", "/my/workdir");
    path = JobUtils.getQueryWorkDir(conf);
    assertEquals("/my/workdir/bq-hive-hive-query-id-query123", path.toString());
  }

  @Test
  public void testGetJobDetailsFilePath() {
    Configuration conf = new Configuration();
    conf.set("hive.query.id", "query123");
    conf.set("hadoop.tmp.dir", "/tmp");
    String hmsDbTable = "default.mytable";
    Path jobDetailsFilePath = JobUtils.getJobDetailsFilePath(conf, hmsDbTable);
    assertEquals(
        "/tmp/bq-hive-hive-query-id-query123/default.mytable/job-details.json",
        jobDetailsFilePath.toString());
  }

  @Test
  public void testGetTaskWriterOutputFile() {
    Configuration conf = new Configuration();
    conf.set("hive.query.id", "query123");
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
        "^/hadoop-tmp/bq-hive-hive-query-id-query123/default.mytable/output/myproject_mydataset_mytable_abcd1234_w\\d+\\.jpeg";
    assertThat(path.toString(), matchesPattern(pattern));
  }

  /**
   * Same as {@link #testGetTaskWriterOutputFile} but with the ".hive-staging" directory, when the
   * `mapreduce.output.fileoutputformat.outputdir` conf property is set. This happens when using
   * Spark SQL.
   */
  @Test
  public void testGetTaskWriterOutputFileWithHiveStagingDir() {
    Configuration conf = new Configuration();
    conf.set("hive.query.id", "query123");
    conf.set("hadoop.tmp.dir", "/hadoop-tmp/");
    conf.set(
        "mapreduce.output.fileoutputformat.outputdir", "/abc/def/.hive-staging-xyz/-ext-10000/");
    JobDetails jobDetails = new JobDetails();
    jobDetails.setTableProperties(new Properties());
    jobDetails.getTableProperties().put("name", "default.mytable");
    TableId tableId = TableId.of("myproject", "mydataset", "mytable");
    jobDetails.setTableId(tableId);
    String taskID = "abcd1234";
    String writerId = WriterRegistry.getWriterId();
    Path path = JobUtils.getTaskWriterOutputFile(conf, jobDetails, taskID, writerId, "jpeg");
    String pattern =
        "^/hadoop-tmp/bq-hive-hive-query-id-query123/default.mytable/.hive-staging-xyz/-ext-10000/myproject_mydataset_mytable_abcd1234_w\\d+\\.jpeg";
    assertThat(path.toString(), matchesPattern(pattern));
  }
}
