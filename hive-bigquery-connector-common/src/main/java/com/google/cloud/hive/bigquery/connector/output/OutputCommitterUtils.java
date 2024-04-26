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
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;

public class OutputCommitterUtils {

  public static void commitJob(Configuration conf, JobDetails jobDetails) throws IOException {
    if (jobDetails.getWriteMethod().equals(HiveBigQueryConfig.WRITE_METHOD_DIRECT)) {
      DirectOutputCommitter.commitJob(conf, jobDetails);
    } else {
      IndirectOutputCommitter.commitJob(conf, jobDetails);
    }
    jobDetails.cleanUp(conf);
  }

  public static void commitJob(Configuration conf) throws IOException {
    Set<String> outputTables = getOutputTables(conf);
    for (String hmsDbTableName : outputTables) {
      if (hmsDbTableName.equals(conf.get("name"))) {
        JobDetails jobDetails = JobDetails.readJobDetailsFile(conf, hmsDbTableName);
        commitJob(conf, jobDetails);
      }
    }
  }

  public static void abortJob(Configuration conf, String hmsDbTableName) throws IOException {
    JobDetails jobDetails = JobDetails.readJobDetailsFile(conf, hmsDbTableName);
    try {
      if (jobDetails.getWriteMethod().equals(HiveBigQueryConfig.WRITE_METHOD_DIRECT)) {
        DirectOutputCommitter.abortJob(conf, jobDetails);
      }
      // Note: The IndirectOutputCommitter doesn't have an abortJob() method.
    } finally {
      jobDetails.cleanUp(conf);
    }
  }

  public static void abortJob(Configuration conf) throws IOException {
    Set<String> outputTables = getOutputTables(conf);
    for (String hmsDbTableName : outputTables) {
      if (hmsDbTableName.equals(conf.get("name"))) {
        abortJob(conf, hmsDbTableName);
      }
    }
  }

  /** Returns the list of output tables for the current job. */
  public static Set<String> getOutputTables(Configuration conf) {
    String outputTables = conf.get(HiveBigQueryConfig.OUTPUT_TABLES_KEY);
    if (outputTables == null) {
      return Collections.emptySet();
    }
    return Sets.newHashSet(HiveBigQueryConfig.OUTPUT_TABLE_NAMES_SPLITTER.split(outputTables));
  }
}
