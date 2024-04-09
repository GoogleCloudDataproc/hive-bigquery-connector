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
package org.apache.hadoop.hive.metastore.conf;

/**
 * Hive 3 has a MetastoreConf class but older versions don't. HiveRunner requires it, so we define
 * it here with the values & methods used by HiveRunner.
 */
public class MetastoreConf {
  public enum ConfVars {
    HIVE_IN_TEST("hive.in.test", "hive.in.test", false, "internal usage only, true in test mode");
    private final String varname;
    private final String hiveName;
    private final Object defaultVal;
    private final boolean caseSensitive;
    private final String description;

    ConfVars(String varname, String hiveName, boolean defaultVal, String description) {
      this.varname = varname;
      this.hiveName = hiveName;
      this.defaultVal = defaultVal;
      caseSensitive = false;
      this.description = description;
    }

    public String getVarname() {
      return varname;
    }
  }
}
