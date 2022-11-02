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
package com.google.cloud.hive.bigquery.connector.input;

import java.util.List;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import repackaged.by.hivebqconnector.com.google.common.collect.ImmutableList;

/**
 * Overrides ExprNodeConstantDesc to make sure filter values (in a WHERE clause) are properly
 * formatted to work with BigQuery.
 */
public class BigQueryConstantDesc extends ExprNodeConstantDesc {

  private static final long serialVersionUID = 1L;
  private static final List<String> SIMPLE_TYPES =
      ImmutableList.of(
          "tinyint",
          "smallint",
          "int",
          "bigint",
          "float",
          "double",
          "char",
          "varchar",
          "string",
          "boolean");

  public BigQueryConstantDesc(TypeInfo typeInfo, Object value) {
    super(typeInfo, value);
  }

  /** Format the value of the predicate (.e. WHERE clause item) to be compatible with BigQuery. */
  private static String formatPredicateValue(TypeInfo typeInfo, Object value) {
    String typeName = typeInfo.getTypeName();
    if (value == null) {
      return "NULL";
    }
    if (typeName.equals("string") || (typeInfo instanceof BaseCharTypeInfo)) {
      return "'" + value + "'";
    }
    if (typeName.equals("date")) {
      return "DATE('" + value + "')";
    }
    if (typeInfo.getTypeName().equals("timestamp")) {
      return "TIMESTAMP('" + value + "')";
    }
    if (SIMPLE_TYPES.contains(typeName) || typeName.startsWith("decimal(")) {
      return value.toString();
    }
    throw new RuntimeException("Unsupported predicate type: " + typeName);
  }

  @Override
  public String getExprString() {
    if (this.typeInfo.getCategory() == ObjectInspector.Category.PRIMITIVE) {
      return formatPredicateValue(this.typeInfo, this.getValue());
    } else {
      throw new RuntimeException("Unsupported predicate type: " + this.typeInfo.getTypeName());
    }
  }

  public static BigQueryConstantDesc translate(ExprNodeConstantDesc constantDesc) {
    return new BigQueryConstantDesc(constantDesc.getTypeInfo(), constantDesc.getValue());
  }
}
