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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** Various filesystem utilities. */
public class FileSystemUtils {

  /** Retrieves the list of files that are in the given directory. */
  public static List<String> getFiles(
      Configuration conf, Path dir, String namePrefix, String extension) throws IOException {
    FileSystem fs = dir.getFileSystem(conf);
    List<String> filePaths = new ArrayList<>();
    FileStatus[] fileStatuses;
    try {
      fileStatuses = fs.listStatus(dir);
    } catch (FileNotFoundException e) {
      return filePaths;
    }
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.getLen() < 1
          || !fileStatus.getPath().getName().startsWith(namePrefix)
          || !FilenameUtils.getExtension(fileStatus.getPath().getName()).equals(extension)) {
        continue;
      }
      filePaths.add(fileStatus.getPath().toString());
    }
    return filePaths;
  }

  /** Delete files in a path. */
  public static void deleteFilesOnExit(Configuration conf, Path dir) throws IOException {
    FileSystem fs = dir.getFileSystem(conf);
    if (fs.exists(dir)) {
      fs.deleteOnExit(dir);
    }
  }

  /** Utility to read a file from disk. */
  public static String readFile(Configuration conf, Path path) throws IOException {
    FSDataInputStream inputStream = path.getFileSystem(conf).open(path);
    String result = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
    inputStream.close();
    return result;
  }
}
