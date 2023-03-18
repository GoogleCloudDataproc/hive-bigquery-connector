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

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

/**
 * Wraps the inner UDF with parentheses. This is required in some cases where BigQuery needs extra
 * parentheses. For example: Hive simplifies "(my_col IS NULL) IS FALSE" to "my_col IS NULL IS
 * FALSE", which is not valid in BigQuery. So this UDF forces it to become "((my_col IS NULL) IS
 * FALSE)" instead.
 */
public class BigQueryUDFWrapParentheses extends BigQueryUDFBase {

  protected GenericUDF innerUDF;

  public BigQueryUDFWrapParentheses(GenericUDF innerUDF) {
    this.innerUDF = innerUDF;
  }

  @Override
  public String getDisplayString(String[] children) {
    return String.format("(%s)", innerUDF.getDisplayString(children));
  }
}
