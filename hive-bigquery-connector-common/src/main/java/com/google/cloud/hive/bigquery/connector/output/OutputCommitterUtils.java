/*
 * Copyright 2023 Google Inc. All Rights Reserved.
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
package com.google.cloud.hive.bigquery.connector.output;

import com.google.cloud.hive.bigquery.connector.JobDetails;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.output.direct.DirectOutputCommitter;
import com.google.cloud.hive.bigquery.connector.output.indirect.IndirectOutputCommitter;
import com.google.cloud.hive.bigquery.connector.utils.FileSystemUtils;
import com.google.cloud.hive.bigquery.connector.utils.JobUtils;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class OutputCommitterUtils {

  public static void commitJob(Configuration conf, JobDetails jobDetails) throws IOException {
    try {
      if (jobDetails.getWriteMethod().equals(HiveBigQueryConfig.WRITE_METHOD_DIRECT)) {
        DirectOutputCommitter.commitJob(conf, jobDetails);
      } else {
        IndirectOutputCommitter.commitJob(conf, jobDetails);
      }
    } finally {
      jobDetails.cleanUp(conf);
    }
  }

  public static void commitJob(Configuration conf) throws IOException {
    Path workDir = JobUtils.getQueryWorkDir(conf);
    List<Path> jobDetailsFiles =
        FileSystemUtils.findFilesRecursively(conf, workDir, HiveBigQueryConfig.JOB_DETAILS_FILE);
    for (Path jobDetailsFile : jobDetailsFiles) {
      JobDetails jobDetails = JobDetails.readJobDetailsFile(conf, jobDetailsFile);
      if (jobDetails.getTableProperties().get("name").equals(conf.get("name"))) {
        commitJob(conf, jobDetails);
      }
    }
  }

  public static void abortJob(Configuration conf) throws IOException {
    Path workDir = JobUtils.getQueryWorkDir(conf);
    List<Path> jobDetailsFiles =
        FileSystemUtils.findFilesRecursively(conf, workDir, HiveBigQueryConfig.JOB_DETAILS_FILE);
    for (Path jobDetailsFile : jobDetailsFiles) {
      JobDetails jobDetails = JobDetails.readJobDetailsFile(conf, jobDetailsFile);
      try {
        if (jobDetails.getWriteMethod().equals(HiveBigQueryConfig.WRITE_METHOD_DIRECT)) {
          DirectOutputCommitter.abortJob(conf, jobDetails);
        }
        // Note: The IndirectOutputCommitter doesn't have an abortJob() method.
      } finally {
        jobDetails.cleanUp(conf);
      }
    }
  }
}
