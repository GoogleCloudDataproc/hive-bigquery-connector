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

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * Translates the TO_DATE() Hive function, which extracts the date from the given value. This
 * differs from the DATE() Hive function, which *casts* the value to a date -- that use case is
 * handled by {@link BigQueryUDFCastDate}.
 */
public class BigQueryUDFDate extends BigQueryUDFBase {

  @Override
  public String getDisplayString(String[] children) {
    if (expr.getChildren().get(0).getTypeInfo().equals(TypeInfoFactory.stringTypeInfo)) {
      // Convert the string to TIMESTAMP first as DATE() doesn't accept string parameters
      // https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators#date
      return String.format("DATE(TIMESTAMP(%s))", children[0]);
    } else {
      return String.format("DATE(%s)", children[0]);
    }
  }
}
