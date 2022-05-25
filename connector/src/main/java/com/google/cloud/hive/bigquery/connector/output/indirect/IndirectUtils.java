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
package com.google.cloud.hive.bigquery.connector.output.indirect;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.hive.bigquery.connector.Constants;
import com.google.cloud.hive.bigquery.connector.config.RunConf.Config;
import com.google.cloud.hive.bigquery.connector.utils.HiveUtils;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskAttemptID;
import repackaged.by.hivebqconnector.com.google.common.collect.ImmutableList;

public class IndirectUtils {

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
  public static boolean hasGcsWriteAccess(String gcsURI) {
    String bucket = extractBucketNameFromGcsUri(gcsURI);
    Storage storage = StorageOptions.newBuilder().build().getService();
    List<Boolean> booleans;
    try {
      booleans = storage.testIamPermissions(bucket, ImmutableList.of("storage.objects.create"));
    } catch (StorageException e) {
      return false;
    }
    return !booleans.contains(false);
  }

  /**
   * Returns the name of the temporary working directory in GCS where the temporary Avro files will
   * be stored.
   */
  public static Path getGcsTempDir(Configuration conf, String gcsPathBase) {
    String base = StringUtils.removeEnd(gcsPathBase, "/");
    return new Path(
        String.format(
            "%s/%s%s", base, Config.WORK_DIR_NAME_PREFIX.get(conf), HiveUtils.getHiveId(conf)));
  }

  /** Deletes the directory on GCS that contains the temporary Avro files for the job. */
  public static void deleteGcsTempDir(Configuration conf, String gcsPathBase) throws IOException {
    Path dir = getGcsTempDir(conf, gcsPathBase);
    FileSystem fs = dir.getFileSystem(conf);
    if (fs.exists(dir)) {
      fs.deleteOnExit(dir);
    }
  }

  /** Return the name prefix for the temp Avro file. */
  public static String getTaskTempAvroFileNamePrefix(TableId tableId) {
    return String.format(
        "%s_%s_%s",
        tableId.getProject(), tableId.getDataset(), tableId.getTable().replace("$", "__"));
  }

  /**
   * Returns the name of a temporary Avro file name where the task will write its output data to.
   * The file will actually be loaded into BigQuery later at the end of the job.
   *
   * @return Fully Qualified temporary table path on GCS
   */
  public static Path getTaskAvroTempFile(
      Configuration conf, TableId tableId, String gcsPathBase, TaskAttemptID taskAttemptID) {
    return new Path(
        getGcsTempDir(conf, gcsPathBase),
        String.format(
            "%s_%s.%s",
            getTaskTempAvroFileNamePrefix(tableId),
            taskAttemptID.getTaskID(),
            Constants.LOAD_FILE_EXTENSION));
  }
}
