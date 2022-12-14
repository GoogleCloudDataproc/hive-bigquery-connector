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

import com.google.cloud.hive.bigquery.connector.Constants;
import com.google.cloud.hive.bigquery.connector.input.udfs.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.*;

public class BigQueryFilters {

    /**
     * Translates the given filter expression (from a WHERE clause) to be compatible with BigQuery.
     */
    public static ExprNodeDesc translateFilters(ExprNodeDesc filterExpr) {
        // Check if it's a function
        if (filterExpr instanceof ExprNodeGenericFuncDesc) {
            ExprNodeGenericFuncDesc function = ((ExprNodeGenericFuncDesc) filterExpr);

            // UDF conversion
            if (function.getGenericUDF() instanceof GenericUDFDateDiff) {
                function.setGenericUDF(new BigQueryUDFDateDiff());
            } else if (function.getGenericUDF() instanceof GenericUDFDateSub) {
                function.setGenericUDF(new BigQueryUDFDateSub());
            } else if (function.getGenericUDF() instanceof GenericUDFDateAdd) {
                function.setGenericUDF(new BigQueryUDFDateAdd());
            } else if (function.getGenericUDF() instanceof GenericUDFOPMod) {
                function.setGenericUDF(new BigQueryUDFMod());
            } else if (function.getGenericUDF() instanceof GenericUDFRegExp) {
                function.setGenericUDF(new BigQueryUDFRegExpContains());
            }
            // Translate the children parameters
            List<ExprNodeDesc> translatedChildren = new ArrayList<>();
            for (ExprNodeDesc child : filterExpr.getChildren()) {
                translatedChildren.add(translateFilters(child));
            }
            function.setChildren(translatedChildren);
            return filterExpr;
        }
        // Check if it's a column
        if (filterExpr instanceof ExprNodeColumnDesc) {
            ExprNodeColumnDesc columnDesc = ((ExprNodeColumnDesc) filterExpr);
            if (columnDesc.getColumn().equalsIgnoreCase(Constants.PARTITION_TIME_PSEUDO_COLUMN)) {
                columnDesc.setColumn(Constants.PARTITION_TIME_PSEUDO_COLUMN);
            } else if (columnDesc.getColumn().equalsIgnoreCase(Constants.PARTITION_DATE_PSEUDO_COLUMN)) {
                columnDesc.setColumn(Constants.PARTITION_DATE_PSEUDO_COLUMN);
            }
            return columnDesc;
        }
        // Check if it's a constant value
        if (filterExpr instanceof ExprNodeConstantDesc) {
            // Convert the ExprNodeConstantDesc to a BigQueryConstantDesc
            // to make sure the value properly formatted for BigQuery.
            return BigQueryConstantDesc.translate((ExprNodeConstantDesc) filterExpr);
        }
        throw new RuntimeException("Unexpected filter type: " + filterExpr);
    }
}