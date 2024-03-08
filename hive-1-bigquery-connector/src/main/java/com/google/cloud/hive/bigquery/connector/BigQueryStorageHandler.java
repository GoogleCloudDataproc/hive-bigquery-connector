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
package com.google.cloud.hive.bigquery.connector;

import com.google.cloud.hive.bigquery.connector.output.PostExecHook;
import com.google.cloud.hive.bigquery.connector.output.PreInsertHook;
import java.util.Map;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.AbstractSerDe;

public class BigQueryStorageHandler extends BigQueryStorageHandlerBase {

  @Override
  public HiveMetaHook getMetaHook() {
    return new OldAPIMetaHook(conf);
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    super.configureOutputJobProperties(tableDesc, jobProperties);

    // In Hive 1, the metahook doesn't have a `preInsertTable()` method, so we use a
    // pre-execution hook instead
    addExecHook(ConfVars.PREEXECHOOKS.varname, PreInsertHook.class);

    String engine = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).toLowerCase();
    if ((engine.equals("tez"))) {
      // Tez does not use the OutputCommitter (regardless of the Hive versions).
      // So with Hive 2 and 3, we override and use the metahook's `commitInsertTable()` method.
      // However, with Hive 1, that method isn't available. So we set up a post execution hook to
      // commit the writes.
      addExecHook(ConfVars.POSTEXECHOOKS.varname, PostExecHook.class);
    }
  }

  @Override
  public Class<? extends AbstractSerDe> getSerDeClass() {
    return BigQuerySerDe.class;
  }
}
