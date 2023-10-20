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
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import com.google.cloud.hive.bigquery.connector.input.udfs.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.udf.generic.*;

public abstract class BigQueryFilters {

  /** Translates the given filter expression to be compatible with BigQuery. */
  public static ExprNodeDesc translateFilters(ExprNodeDesc filterExpr, Configuration conf) {
    // Check if it's a function
    if (filterExpr instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc function = ((ExprNodeGenericFuncDesc) filterExpr);
      GenericUDF udf = HiveCompat.getInstance().convertUDF(function, conf);
      if (udf == null) {
        // Unsupported UDF. Bail.
        return null;
      }
      if (udf instanceof BigQueryUDFBase) {
        ((BigQueryUDFBase) udf).setExpr(function);
      }
      function.setGenericUDF(udf);

      // Translate the children parameters
      List<ExprNodeDesc> translatedChildren = new ArrayList<>();
      for (ExprNodeDesc child : filterExpr.getChildren()) {
        ExprNodeDesc translatedChild = translateFilters(child, conf);
        if (translatedChild == null) {
          // Child contains an unsupported UDF
          if (udf instanceof GenericUDFOPAnd) {
            // This is an AND operator, so we can just ignore the invalid
            // child and move on to the next.
            continue;
          } else {
            // Bail
            return null;
          }
        }
        translatedChildren.add(translatedChild);
      }
      if (udf instanceof GenericUDFOPAnd) {
        if (translatedChildren.size() == 0) {
          // The AND operator has no children. So the whole branch is invalid. Bail.
          return null;
        }
        if (translatedChildren.size() == 1) {
          // There's only 1 item for the AND operator, so just return the item itself.
          return translatedChildren.get(0);
        }
      }
      function.setChildren(translatedChildren);
      return filterExpr;
    }

    // Check if it's a column
    if (filterExpr instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc columnDesc = ((ExprNodeColumnDesc) filterExpr);
      if (columnDesc
          .getColumn()
          .equalsIgnoreCase(HiveBigQueryConfig.PARTITION_TIME_PSEUDO_COLUMN)) {
        columnDesc.setColumn(HiveBigQueryConfig.PARTITION_TIME_PSEUDO_COLUMN);
      } else if (columnDesc
          .getColumn()
          .equalsIgnoreCase(HiveBigQueryConfig.PARTITION_DATE_PSEUDO_COLUMN)) {
        columnDesc.setColumn(HiveBigQueryConfig.PARTITION_DATE_PSEUDO_COLUMN);
      }
      return columnDesc;
    }
    // Check if it's a constant value
    if (filterExpr instanceof ExprNodeConstantDesc) {
      // Convert the ExprNodeConstantDesc to a BigQueryConstantDesc
      // to make sure the value is properly formatted for BigQuery.
      ExprNodeConstantDesc constantDesc = (ExprNodeConstantDesc) filterExpr;
      return new BigQueryConstantDesc(constantDesc.getTypeInfo(), constantDesc.getValue());
    }
    if (filterExpr instanceof ExprNodeFieldDesc) {
      ExprNodeFieldDesc fieldDesc = ((ExprNodeFieldDesc) filterExpr);
      return new BigQueryFieldDesc(fieldDesc);
    }
    throw new RuntimeException("Unexpected filter type: " + filterExpr);
  }
}
