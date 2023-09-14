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
package com.google.cloud.hive.bigquery.connector.input.udfs;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.cloud.hive.bigquery.connector.input.BigQueryConstantDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.jupiter.api.Test;

public class Hive3BigQueryConstantDescTest {
  @Test
  public void testTimestampLocalTZ() {
    BigQueryConstantDesc desc =
        new BigQueryConstantDesc(
            TypeInfoFactory.timestampLocalTZTypeInfo, "2010-10-10 1:2:3.123456");
    assertEquals("TIMESTAMP'2010-10-10 1:2:3.123456'", desc.getExprString());
  }
}
