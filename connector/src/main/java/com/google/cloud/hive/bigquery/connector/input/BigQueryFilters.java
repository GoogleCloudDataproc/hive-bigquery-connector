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

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

public class BigQueryFilters {

  /**
   * Translates the given filter expression (from a WHERE clause) to be compatible with BigQuery.
   */
  public static ExprNodeDesc translateFilters(ExprNodeDesc filterExpr) {
    if (filterExpr instanceof ExprNodeGenericFuncDesc) {
      // Let's go one level deeper
      List<ExprNodeDesc> translatedChildren = new ArrayList<>();
      for (ExprNodeDesc child : filterExpr.getChildren()) {
        translatedChildren.add(translateFilters(child));
      }
      ((ExprNodeGenericFuncDesc) filterExpr).setChildren(translatedChildren);
      return filterExpr;
    }
    if (filterExpr instanceof ExprNodeColumnDesc) {
      return filterExpr;
    }
    if (filterExpr instanceof ExprNodeConstantDesc) {
      // Convert the ExprNodeConstantDesc to a BigQueryConstantDesc
      // to make sure the value properly formatted for BigQuery.
      return BigQueryConstantDesc.translate((ExprNodeConstantDesc) filterExpr);
    }
    throw new RuntimeException("Unexpected filter type: " + filterExpr);
  }
}
