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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.cloud.hive.bigquery.connector.input.BigQueryFilters;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.*;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToChar;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToVarchar;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.jupiter.api.Test;

public class BigQueryUDFToStringTest {

  @Test
  public void testString() {
    GenericUDF[] udfs =
        new GenericUDF[] {
          new GenericUDFBridge(
              UDFToString.class.getSimpleName(), false, UDFToString.class.getName()),
          new GenericUDFToVarchar(),
          new GenericUDFToChar(),
        };
    for (GenericUDF udf : udfs) {
      ExprNodeGenericFuncDesc func = new ExprNodeGenericFuncDesc();
      func.setGenericUDF(udf);
      List<ExprNodeDesc> children =
          new ArrayList<>(
              Collections.singletonList(
                  new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, "99")));
      func.setChildren(children);
      String expression = BigQueryFilters.translateFilters(func).getExprString();
      assertEquals("CAST(99 AS STRING)", expression);
    }
  }
}
