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

import java.util.Arrays;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.udf.*;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.jupiter.api.Test;

public class UDFTest extends UDFTestBase {

  // Other tests are from the super-class

  @Test
  public void testCastTimestamp() {
    String expression =
        translateUDF(
            new GenericUDFToTimestampLocalTZ(),
            Arrays.asList(new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "2010-10-10")));
    assertEquals("CAST('2010-10-10' AS TIMESTAMP)", expression);
  }

  @Test
  public void testYear() {
    String expression =
        translateUDF(
            new UDFYear(),
            Arrays.asList(
                new ExprNodeConstantDesc(TypeInfoFactory.timestampTypeInfo, "2010-10-10")));
    assertEquals("EXTRACT(YEAR FROM DATETIME'2010-10-10')", expression);
  }

  @Test
  public void testMonth() {
    String expression =
        translateUDF(
            new UDFMonth(),
            Arrays.asList(
                new ExprNodeConstantDesc(TypeInfoFactory.timestampTypeInfo, "2010-10-10")));
    assertEquals("EXTRACT(MONTH FROM DATETIME'2010-10-10')", expression);
  }

  @Test
  public void testDayOfMonth() {
    String expression =
        translateUDF(
            new UDFDayOfMonth(),
            Arrays.asList(
                new ExprNodeConstantDesc(TypeInfoFactory.timestampTypeInfo, "2010-10-10")));
    assertEquals("EXTRACT(DAY FROM DATETIME'2010-10-10')", expression);
  }

  @Test
  public void testHour() {
    String expression =
        translateUDF(
            new UDFHour(),
            Arrays.asList(
                new ExprNodeConstantDesc(TypeInfoFactory.timestampTypeInfo, "2010-10-10")));
    assertEquals("EXTRACT(HOUR FROM DATETIME'2010-10-10')", expression);
  }

  @Test
  public void testMinute() {
    String expression =
        translateUDF(
            new UDFMinute(),
            Arrays.asList(
                new ExprNodeConstantDesc(TypeInfoFactory.timestampTypeInfo, "2010-10-10")));
    assertEquals("EXTRACT(MINUTE FROM DATETIME'2010-10-10')", expression);
  }

  @Test
  public void testSecond() {
    String expression =
        translateUDF(
            new UDFSecond(),
            Arrays.asList(
                new ExprNodeConstantDesc(TypeInfoFactory.timestampTypeInfo, "2010-10-10")));
    assertEquals("EXTRACT(SECOND FROM DATETIME'2010-10-10')", expression);
  }

  @Test
  public void testIsTrue() {
    String expression =
        translateUDF(
            new GenericUDFOPTrue(),
            Arrays.asList(new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "abc")));
    assertEquals("('abc' is true)", expression);
  }

  @Test
  public void testIsNotTrue() {
    String expression =
        translateUDF(
            new GenericUDFOPNotTrue(),
            Arrays.asList(new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "abc")));
    assertEquals("('abc' is not true)", expression);
  }

  @Test
  public void testIsFalse() {
    String expression =
        translateUDF(
            new GenericUDFOPFalse(),
            Arrays.asList(new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "abc")));
    assertEquals("('abc' is false)", expression);
  }

  @Test
  public void testIsNotFalse() {
    String expression =
        translateUDF(
            new GenericUDFOPNotFalse(),
            Arrays.asList(new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "abc")));
    assertEquals("('abc' is not false)", expression);
  }
}
