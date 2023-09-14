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
package com.google.cloud.hive.bigquery.connector.input;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Timestamp;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.jupiter.api.Test;

public class BigQueryConstantDescTest {

  @Test
  public void testString() {
    BigQueryConstantDesc desc = new BigQueryConstantDesc(TypeInfoFactory.stringTypeInfo, "abc");
    assertEquals("'abc'", desc.getExprString());
  }

  @Test
  public void testTinyInt() {
    BigQueryConstantDesc desc = new BigQueryConstantDesc(TypeInfoFactory.byteTypeInfo, "123");
    assertEquals("123", desc.getExprString());
  }

  @Test
  public void testSmallInt() {
    BigQueryConstantDesc desc = new BigQueryConstantDesc(TypeInfoFactory.shortTypeInfo, "123");
    assertEquals("123", desc.getExprString());
  }

  @Test
  public void testInt() {
    BigQueryConstantDesc desc = new BigQueryConstantDesc(TypeInfoFactory.intTypeInfo, "123");
    assertEquals("123", desc.getExprString());
  }

  @Test
  public void testBigInt() {
    BigQueryConstantDesc desc = new BigQueryConstantDesc(TypeInfoFactory.longTypeInfo, "123");
    assertEquals("123", desc.getExprString());
  }

  @Test
  public void testFloat() {
    BigQueryConstantDesc desc = new BigQueryConstantDesc(TypeInfoFactory.floatTypeInfo, "4.2");
    assertEquals("4.2", desc.getExprString());
  }

  @Test
  public void testDouble() {
    BigQueryConstantDesc desc = new BigQueryConstantDesc(TypeInfoFactory.doubleTypeInfo, "4.2");
    assertEquals("4.2", desc.getExprString());
  }

  @Test
  public void testChar() {
    BigQueryConstantDesc desc = new BigQueryConstantDesc(TypeInfoFactory.charTypeInfo, "abc");
    assertEquals("'abc'", desc.getExprString());
  }

  @Test
  public void testVarChar() {
    BigQueryConstantDesc desc = new BigQueryConstantDesc(TypeInfoFactory.varcharTypeInfo, "abc");
    assertEquals("'abc'", desc.getExprString());
  }

  @Test
  public void testBoolean() {
    BigQueryConstantDesc desc = new BigQueryConstantDesc(TypeInfoFactory.booleanTypeInfo, "true");
    assertEquals("true", desc.getExprString());
  }

  @Test
  public void testNull() {
    BigQueryConstantDesc desc = new BigQueryConstantDesc(TypeInfoFactory.stringTypeInfo, null);
    assertEquals("NULL", desc.getExprString());
  }

  @Test
  public void testBinary() {
    BigQueryConstantDesc desc =
        new BigQueryConstantDesc(TypeInfoFactory.binaryTypeInfo, "abc".getBytes());
    assertEquals("B'abc'", desc.getExprString());
  }

  @Test
  public void testDecimal() {
    BigQueryConstantDesc desc = new BigQueryConstantDesc(TypeInfoFactory.decimalTypeInfo, "4.2");
    assertEquals("4.2", desc.getExprString());
  }

  @Test
  public void testDate() {
    BigQueryConstantDesc desc =
        new BigQueryConstantDesc(TypeInfoFactory.dateTypeInfo, "2010-10-10");
    assertEquals("DATE'2010-10-10'", desc.getExprString());
  }

  @Test
  public void testTimestamp() {
    assertEquals(
        "DATETIME'2010-10-10 01:02:03.123456'",
        new BigQueryConstantDesc(
                TypeInfoFactory.timestampTypeInfo, Timestamp.valueOf("2010-10-10 1:2:3.123456"))
            .getExprString());
    assertEquals(
        "DATETIME'2022-01-06 00:00:00.0'",
        new BigQueryConstantDesc(
                TypeInfoFactory.timestampTypeInfo, Timestamp.valueOf("2022-01-06 00:00:00"))
            .getExprString());
  }

  @Test
  public void testIntervalDayTime() {
    BigQueryConstantDesc desc =
        new BigQueryConstantDesc(
            TypeInfoFactory.intervalDayTimeTypeInfo,
            HiveIntervalDayTime.valueOf("+1 2:3:4.123456"));
    assertEquals("INTERVAL '93784.123456' SECOND", desc.getExprString());
  }
}
