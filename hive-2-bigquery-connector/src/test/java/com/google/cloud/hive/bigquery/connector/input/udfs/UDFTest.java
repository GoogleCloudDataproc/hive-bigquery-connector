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

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.udf.UDFDayOfWeek;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.jupiter.api.Test;

public class UDFTest extends UDFTestBase {

  // Other tests are from the super-class

  @Test
  public void testRegExprContains() {
    String expression =
        translateUDF(
            new GenericUDFRegExp(),
            Arrays.asList(
                new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "abcd"),
                new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "xyz")));
    assertEquals("REGEXP_CONTAINS('abcd', r'xyz')", expression);
  }

  @Test
  public void testQuarter() {
    String expression =
        translateUDF(
            new GenericUDFQuarter(),
            Arrays.asList(
                new ExprNodeConstantDesc(TypeInfoFactory.timestampTypeInfo, "2010-10-10")));
    assertEquals("EXTRACT(QUARTER FROM DATETIME'2010-10-10')", expression);
  }

  @Test
  public void testDayOfWeek() {
    String expression =
        translateUDF(
            new GenericUDFBridge(
                UDFDayOfWeek.class.getSimpleName(), false, UDFDayOfWeek.class.getName()),
            Arrays.asList(
                new ExprNodeConstantDesc(TypeInfoFactory.timestampTypeInfo, "2010-10-10")));
    assertEquals("EXTRACT(DAYOFWEEK FROM DATETIME'2010-10-10')", expression);
  }
}
