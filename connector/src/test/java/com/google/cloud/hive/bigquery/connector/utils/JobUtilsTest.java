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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.hive.bigquery.connector.JobDetails;
import com.google.cloud.hive.bigquery.connector.output.WriterRegistry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskAttemptID;
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
    assertEquals("/tmp/bq-hive-query123", path.toString());
    conf.set("bq.work.dir.parent.path", "/my/workdir");
    path = JobUtils.getQueryWorkDir(conf);
    assertEquals("/my/workdir/bq-hive-query123", path.toString());
  }

  @Test
  public void testGetJobDetailsFilePath() {
    Configuration conf = new Configuration();
    conf.set("hive.query.id", "query123");
    conf.set("hadoop.tmp.dir", "/tmp");
    String hmsDbTable = "default.mytable";
    Path jobDetailsFilePath = JobUtils.getJobDetailsFilePath(conf, hmsDbTable);
    assertEquals(
        "/tmp/bq-hive-query123/default.mytable/job-details.json", jobDetailsFilePath.toString());
  }

  @Test
  public void testGetTaskWriterOutputFile() {
    JobDetails jobDetails = new JobDetails();
    String tmp = "/tmp/bq-hive-query123/default.mytable";
    jobDetails.setJobTempOutputPath(new Path(tmp));
    TableId tableId = TableId.of("myproject", "mydataset", "mytable");
    jobDetails.setTableId(tableId);
    TaskAttemptID taskAttemptID = new TaskAttemptID();
    String writerId = WriterRegistry.getWriterId();
    Path path = JobUtils.getTaskWriterOutputFile(jobDetails, taskAttemptID, writerId, "json");
    assertEquals(tmp + "/myproject_mydataset_mytable_task__0000_r_000000_w1.json", path.toString());
  }
}
