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

import com.google.cloud.hive.bigquery.connector.HiveCompat;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.*;

/**
 * Overrides ExprNodeConstantDesc to make sure filter values (in a WHERE clause) are properly
 * formatted to work with BigQuery.
 */
public class BigQueryConstantDesc extends ExprNodeConstantDesc {

  private static final long serialVersionUID = 1L;

  public BigQueryConstantDesc(TypeInfo typeInfo, Object value) {
    super(typeInfo, value);
  }

  @Override
  public String getExprString() {
    if (this.typeInfo.getCategory() == ObjectInspector.Category.PRIMITIVE) {
      return HiveCompat.getInstance().formatPredicateValue(this.typeInfo, this.getValue());
    } else {
      throw new RuntimeException("Unsupported predicate type: " + this.typeInfo.getTypeName());
    }
  }
}
