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
package com.google.cloud.hive.bigquery.connector.output.direct;

import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.bigquery.TableId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.junit.jupiter.api.Test;

public class DirectUtilsTest {

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
