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

import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;

/** Converts a nested field's descriptor into a format that BigQuery can process. */
public class BigQueryFieldDesc extends ExprNodeDesc {

  ExprNodeFieldDesc hiveExpr;

  public BigQueryFieldDesc(ExprNodeFieldDesc hiveExpr) {
    this.hiveExpr = hiveExpr;
  }

  @Override
  public ExprNodeDesc clone() {
    return new BigQueryFieldDesc((ExprNodeFieldDesc) hiveExpr.clone());
  }

  @Override
  public boolean isSame(Object o) {
    return (o instanceof BigQueryFieldDesc
        && ((BigQueryFieldDesc) o).hiveExpr.isSame(this.hiveExpr));
  }

  @Override
  public String getExprString() {
    StringBuilder sb = new StringBuilder();
    sb.insert(0, hiveExpr.getFieldName());
    sb.insert(0, ".");
    ExprNodeDesc field = hiveExpr.getDesc();
    do {
      if (field instanceof ExprNodeFieldDesc) {
        sb.insert(0, ((ExprNodeFieldDesc) field).getFieldName());
        sb.insert(0, ".");
        field = ((ExprNodeFieldDesc) field).getDesc();
      }
    } while (field.getChildren() != null);
    sb.insert(0, ((ExprNodeColumnDesc) field).getColumn());
    return sb.toString();
  }
}
