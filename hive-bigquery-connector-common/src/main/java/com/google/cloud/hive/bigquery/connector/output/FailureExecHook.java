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

import com.google.cloud.hive.bigquery.connector.utils.hive.HiveUtils;
import org.apache.hadoop.hive.ql.hooks.Entity.Type;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity.WriteType;

/**
 * This hook is only used with Tez because Tez does not call `OutputCommitter.abortJob()` when a job
 * fails. So we use this hook to replicate the same behavior that `OutputCommitter.abortJob()` would
 * do with the MapReduce engine.
 */
public class FailureExecHook implements ExecuteWithHookContext {

  @Override
  public void run(HookContext hookContext) throws Exception {
    for (WriteEntity entity : hookContext.getOutputs()) {
      if (entity.getType() == Type.TABLE
          && entity.getTable().getStorageHandler() != null
          && (entity.getWriteType() == WriteType.INSERT
              || entity.getWriteType() == WriteType.INSERT_OVERWRITE)
          && entity
              .getTable()
              .getStorageHandler()
              .getClass()
              .getName()
              .equals("com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler")) {
        String hmsDbTableName = HiveUtils.getDbTableName(entity.getTable());
        OutputCommitterUtils.abortJob(hookContext.getConf(), hmsDbTableName);
      }
    }
  }
}
