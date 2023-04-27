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
package com.google.cloud.hive.bigquery.connector.utils;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.connector.common.BigQueryCredentialsSupplier;
import com.google.cloud.hive.bigquery.connector.JobDetails;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.utils.hive.HiveUtils;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskAttemptID;

public class JobUtils {

  static Pattern gcsUriPattern = Pattern.compile("gs://([^/]*)(.*)?");

  /** Retrieves the bucket name from a fully-qualified GCS URI. */
  public static String extractBucketNameFromGcsUri(String gcsURI) {
    Matcher m = gcsUriPattern.matcher(gcsURI);
    if (m.find()) {
      return m.group(1);
    } else {
      throw new RuntimeException("Incorrect GCS URI: " + gcsURI);
    }
  }

  /** Returns true if the logged-in user has access to the given GCS path. */
  public static boolean hasGcsWriteAccess(
      BigQueryCredentialsSupplier credentialsSupplier, String gcsURI) {
    String bucket = extractBucketNameFromGcsUri(gcsURI);
    Storage storage =
        StorageOptions.newBuilder()
            .setCredentials(credentialsSupplier.getCredentials())
            .build()
            .getService();
    List<Boolean> booleans;
    try {
      booleans = storage.testIamPermissions(bucket, ImmutableList.of("storage.objects.create"));
    } catch (StorageException e) {
      return false;
    }
    return !booleans.contains(false);
  }

  /** working directory for a query */
  public static Path getWorkDir(Configuration conf) {
    String parentPath = conf.get(HiveBigQueryConfig.WORK_DIR_PARENT_PATH_KEY);
    if (parentPath == null) {
      parentPath = conf.get(CommonConfigurationKeys.HADOOP_TMP_DIR);
    }
    return getQuerySubDir(conf, parentPath);
  }

  /**
   * Returns the location of the "details" file, which contains strategic details about a job that
   * can be consulted at various stages of the job's execution.
   */
  public static Path getJobDetailsFilePath(Configuration conf, String hmsDbTableName) {
    Path workDir = getWorkDir(conf);
    Path tblWorkPath = new Path(workDir, hmsDbTableName);
    return new Path(tblWorkPath, HiveBigQueryConfig.JOB_DETAILS_FILE);
  }

  private static Path getQuerySubDir(Configuration conf, String pathBase) {
    String base = StringUtils.removeEnd(pathBase, "/");
    return new Path(
        String.format(
            "%s/%s%s",
            base,
            conf.get(
                HiveBigQueryConfig.WORK_DIR_NAME_PREFIX_KEY,
                HiveBigQueryConfig.WORK_DIR_NAME_PREFIX_DEFAULT),
            HiveUtils.getQueryId(conf)));
  }

  public static Path getQueryTempOutputPath(Configuration conf, HiveBigQueryConfig opts) {
    // direct method writes stream ref files in workdir, indirect writes to gcs temp dir.
    if (opts.getWriteMethod().equals(HiveBigQueryConfig.WRITE_METHOD_DIRECT)) {
      return getWorkDir(conf);
    } else {
      String parentPath = opts.getTempGcsPath();
      return getQuerySubDir(conf, parentPath);
    }
  }

  /**
   * Returns the name of a temporary Avro file name where the task writer will write its output data
   * to. The file will actually be loaded into BigQuery later at the end of the job.
   *
   * @return Fully Qualified temporary table path on GCS
   */
  public static Path getTaskWriterOutputFile(
      JobDetails jobDetails, TaskAttemptID taskAttemptID, String writerId, String suffix) {
    return new Path(
        jobDetails.getJobTempOutputPath(),
        String.format(
            "%s_%s_%s.%s",
            getTableIdPrefix(jobDetails.getTableId()),
            taskAttemptID.getTaskID(),
            writerId,
            suffix));
  }

  /** Return the name prefix for the temp file. */
  public static String getTableIdPrefix(TableId tableId) {
    return String.format(
            "%s_%s_%s",
            tableId.getProject(), tableId.getDataset(), tableId.getTable().replace("$", "__"))
        .replace(":", "__");
  }

  public static void deleteJobTempOutput(Configuration conf, JobDetails jobDetails)
      throws IOException {
    FileSystemUtils.deleteFilesOnExit(conf, jobDetails.getJobTempOutputPath());
  }

  /** Deletes the work directory for a table. */
  public static void deleteJobDirOnExit(Configuration conf, String hmsDbTableName)
      throws IOException {
    Path workDir = getWorkDir(conf);
    Path tblWorkPath = new Path(workDir, hmsDbTableName);
    FileSystem fs = tblWorkPath.getFileSystem(conf);
    if (fs.exists(tblWorkPath)) {
      fs.deleteOnExit(tblWorkPath);
    }
  }
}
