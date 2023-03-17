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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryFilters {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryFilters.class);

  // List of UDFs and operators that are exactly the same in Hive and BigQuery
  // TODO: Make sure we make this list as comprehensive as possible
  protected static List<String> identicalUDFs;

  static {
    identicalUDFs = new ArrayList<>();
    for (Class udf :
        new Class[] {
          GenericUDFAbs.class,
          GenericUDFCeil.class,
          GenericUDFFloor.class,
          GenericUDFCoalesce.class,
          GenericUDFCoalesce.class,
          GenericUDFConcat.class,
          GenericUDFOPEqual.class,
          GenericUDFOPNotEqual.class,
          GenericUDFOPGreaterThan.class,
          GenericUDFOPLessThan.class,
          GenericUDFOPEqualOrGreaterThan.class,
          GenericUDFOPEqualOrLessThan.class,
          GenericUDFIn.class,
          GenericUDFBetween.class,
          GenericUDFOPNot.class,
          GenericUDFOPNull.class,
          GenericUDFOPNotNull.class,
          GenericUDFOPTrue.class,
          GenericUDFOPNotTrue.class,
          GenericUDFOPFalse.class,
          GenericUDFOPNotFalse.class,
          GenericUDFOPAnd.class,
          GenericUDFOPOr.class,
          GenericUDFOPPlus.class,
          GenericUDFOPMinus.class,
          GenericUDFOPTrue.class,
          GenericUDFOPFalse.class,
          GenericUDFOPNegative.class,
          GenericUDFOPPositive.class,
          GenericUDFPower.class,
          GenericUDFOPDivide.class,
          GenericUDFOPMultiply.class
        }) {
      identicalUDFs.add(udf.getName());
    }
  }

  /** Converts the Hive UDF to the corresponding BigQuery function */
  protected static GenericUDF convertUDF(GenericUDF udf) {
    if (identicalUDFs.contains(udf.getUdfName())) {
      return udf;
    }
    if (udf instanceof GenericUDFDateDiff) {
      return new BigQueryUDFDateDiff();
    } else if (udf instanceof GenericUDFDateSub) {
      return new BigQueryUDFDateSub();
    } else if (udf instanceof GenericUDFDateAdd) {
      return new BigQueryUDFDateAdd();
    } else if (udf instanceof GenericUDFOPMod) {
      return new BigQueryUDFMod();
    } else if (udf instanceof GenericUDFRegExp) {
      return new BigQueryUDFRegExpContains();
    } else if (udf instanceof GenericUDFToDate) {
      return new BigQueryUDFToDate();
    } else if (udf instanceof GenericUDFTimestamp) {
      return new BigQueryUDFToDatetime();
    } else if (udf instanceof GenericUDFToTimestampLocalTZ) {
      return new BigQueryUDFToTimestamp();
    } else if (udf instanceof GenericUDFToBinary) {
      return new BigQueryUDFToBytes();
    } else if (udf instanceof GenericUDFToVarchar) {
      return new BigQueryUDFToString();
    } else if (udf instanceof GenericUDFToChar) {
      return new BigQueryUDFToString();
    } else if (udf instanceof GenericUDFToDecimal) {
      return new BigQueryUDFToDecimal();
    } else if (udf instanceof GenericUDFBridge) {
      switch (udf.getUdfName()) {
        case "UDFToString":
          return new BigQueryUDFToString();
        case "UDFToLong":
        case "UDFToInteger":
        case "UDFToShort":
        case "UDFToByte":
          return new BigQueryUDFToInt64();
        case "UDFToBoolean":
          return new BigQueryUDFToBoolean();
        case "UDFToFloat":
        case "UDFToDouble":
          return new BigQueryUDFToFloat64();
      }
    }
    // Unsupported UDF
    LOG.info("Unsupported UDF: " + udf.getUdfName());
    return null;
  }

  /**
   * Translates the given filter expression (from a WHERE clause) to be compatible with BigQuery.
   */
  public static ExprNodeDesc translateFilters(ExprNodeDesc filterExpr) {
    // Check if it's a function
    if (filterExpr instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc function = ((ExprNodeGenericFuncDesc) filterExpr);
      GenericUDF udf = convertUDF(function.getGenericUDF());
      if (udf == null) {
        // Unsupported UDF. Bail.
        return null;
      }
      function.setGenericUDF(udf);

      // Translate the children parameters
      List<ExprNodeDesc> translatedChildren = new ArrayList<>();
      for (ExprNodeDesc child : filterExpr.getChildren()) {
        ExprNodeDesc translatedChild = translateFilters(child);
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
