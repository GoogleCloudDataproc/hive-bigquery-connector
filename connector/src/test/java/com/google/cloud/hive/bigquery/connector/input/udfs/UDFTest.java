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

import com.google.cloud.hive.bigquery.connector.input.BigQueryFilters;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.*;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.jupiter.api.Test;

public class UDFTest {

  public String translateUDF(GenericUDF udf, List<ExprNodeDesc> children) {
    ExprNodeGenericFuncDesc func = new ExprNodeGenericFuncDesc();
    func.setGenericUDF(udf);
    func.setChildren(children);
    return BigQueryFilters.translateFilters(func, new Configuration()).getExprString();
  }

  @Test
  public void testDateAdd() {
    String expression =
        translateUDF(
            new GenericUDFDateAdd(),
            Arrays.asList(
                new ExprNodeConstantDesc(TypeInfoFactory.dateTypeInfo, "2010-07-07"),
                new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, "42")));
    assertEquals("DATE_ADD(DATE'2010-07-07', INTERVAL 42 DAY)", expression);
  }

  @Test
  public void testDateDiff() {
    String expression =
        translateUDF(
            new GenericUDFDateDiff(),
            Arrays.asList(
                new ExprNodeConstantDesc(TypeInfoFactory.dateTypeInfo, "2010-07-07"),
                new ExprNodeConstantDesc(TypeInfoFactory.dateTypeInfo, "2008-12-25")));
    assertEquals("DATE_DIFF(DATE'2010-07-07', DATE'2008-12-25', DAY)", expression);
  }

  @Test
  public void testDateSub() {
    String expression =
        translateUDF(
            new GenericUDFDateSub(),
            Arrays.asList(
                new ExprNodeConstantDesc(TypeInfoFactory.dateTypeInfo, "2010-07-07"),
                new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, "42")));
    assertEquals("DATE_SUB(DATE'2010-07-07', INTERVAL 42 DAY)", expression);
  }

  @Test
  public void testMod() {
    String expression =
        translateUDF(
            new GenericUDFOPMod(),
            Arrays.asList(
                new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, "99"),
                new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, "42")));
    assertEquals("MOD(99, 42)", expression);
  }

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
  public void testShiftLeft() {
    String expression =
        translateUDF(
            new GenericUDFBridge(
                UDFOPBitShiftLeft.class.getSimpleName(), false, UDFOPBitShiftLeft.class.getName()),
            Arrays.asList(
                new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, "99"),
                new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, "42")));
    assertEquals("99 << 42", expression);
  }

  @Test
  public void testShiftRight() {
    String expression =
        translateUDF(
            new GenericUDFBridge(
                UDFOPBitShiftRight.class.getSimpleName(),
                false,
                UDFOPBitShiftRight.class.getName()),
            Arrays.asList(
                new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, "99"),
                new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, "42")));
    assertEquals("99 >> 42", expression);
  }

  @Test
  public void testBoolean() {
    String expression =
        translateUDF(
            new GenericUDFBridge(
                UDFToBoolean.class.getSimpleName(), false, UDFToBoolean.class.getName()),
            Arrays.asList(new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "abc")));
    assertEquals("CAST('abc' AS BOOL)", expression);
  }

  @Test
  public void testBytes() {
    String expression =
        translateUDF(
            new GenericUDFToBinary(),
            Arrays.asList(new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "abc")));
    assertEquals("CAST('abc' AS BYTES)", expression);
  }

  @Test
  public void testDate() {
    String expression =
        translateUDF(
            new GenericUDFToDate(),
            Arrays.asList(new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "2010-10-10")));
    assertEquals("CAST('2010-10-10' AS DATE)", expression);
  }

  @Test
  public void testDatetime() {
    String expression =
        translateUDF(
            new GenericUDFTimestamp(),
            Arrays.asList(new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "2010-10-10")));
    assertEquals("CAST('2010-10-10' AS DATETIME)", expression);
  }

  @Test
  public void testDecimal() {
    String expression =
        translateUDF(
            new GenericUDFToDecimal(),
            Arrays.asList(new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "4.2")));
    assertEquals("CAST('4.2' AS BIGNUMERIC)", expression);
  }

  @Test
  public void testFloat64() {
    Class[] udfs = new Class[] {UDFToFloat.class, UDFToDouble.class};
    for (Class udf : udfs) {
      String expression =
          translateUDF(
              new GenericUDFBridge(udf.getSimpleName(), false, udf.getName()),
              Arrays.asList(new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, "99")));
      assertEquals("CAST(99 AS FLOAT64)", expression);
    }
  }

  @Test
  public void testFromHex() {
    String expression =
        translateUDF(
            new GenericUDFBridge(UDFUnhex.class.getSimpleName(), false, UDFUnhex.class.getName()),
            Arrays.asList(new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "abcd")));
    assertEquals("FROM_HEX('abcd')", expression);
  }

  @Test
  public void testToHex() {
    String expression =
        translateUDF(
            new GenericUDFBridge(UDFHex.class.getSimpleName(), false, UDFHex.class.getName()),
            Arrays.asList(new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "abcd")));
    assertEquals("TO_HEX('abcd')", expression);
  }

  @Test
  public void testIfNull() {
    String expression =
        translateUDF(
            new GenericUDFNvl(),
            Arrays.asList(
                new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "abcd"),
                new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "xyz")));
    assertEquals("IFNULL('abcd', 'xyz')", expression);
  }

  @Test
  public void testInt64() {
    Class[] udfs =
        new Class[] {UDFToByte.class, UDFToShort.class, UDFToInteger.class, UDFToLong.class};
    for (Class udf : udfs) {
      String expression =
          translateUDF(
              new GenericUDFBridge(udf.getSimpleName(), false, udf.getName()),
              Arrays.asList(new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, "99")));
      assertEquals("CAST(99 AS INT64)", expression);
    }
  }

  @Test
  public void testString() {
    GenericUDF[] udfs =
        new GenericUDF[] {
          new GenericUDFBridge(
              UDFToString.class.getSimpleName(), false, UDFToString.class.getName()),
          new GenericUDFToVarchar(),
          new GenericUDFToChar(),
        };
    for (GenericUDF udf : udfs) {
      String expression =
          translateUDF(
              udf, Arrays.asList(new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, "99")));
      assertEquals("CAST(99 AS STRING)", expression);
    }
  }

  @Test
  public void testTimestamp() {
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
  public void testQuarter() {
    String expression =
        translateUDF(
            new GenericUDFQuarter(),
            Arrays.asList(
                new ExprNodeConstantDesc(TypeInfoFactory.timestampTypeInfo, "2010-10-10")));
    assertEquals("EXTRACT(QUARTER FROM DATETIME'2010-10-10')", expression);
  }

  @Test
  public void testWeekOfYear() {
    String expression =
        translateUDF(
            new GenericUDFBridge(
                UDFWeekOfYear.class.getSimpleName(), false, UDFWeekOfYear.class.getName()),
            Arrays.asList(
                new ExprNodeConstantDesc(TypeInfoFactory.timestampTypeInfo, "2010-10-10")));
    assertEquals("EXTRACT(WEEK FROM DATETIME'2010-10-10')", expression);
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

  @Test
  public void testIsNull() {
    String expression =
        translateUDF(
            new GenericUDFOPNull(),
            Arrays.asList(new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "abc")));
    assertEquals("('abc' is null)", expression);
  }

  @Test
  public void testIsNotNull() {
    String expression =
        translateUDF(
            new GenericUDFOPNotNull(),
            Arrays.asList(new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "abc")));
    assertEquals("('abc' is not null)", expression);
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
