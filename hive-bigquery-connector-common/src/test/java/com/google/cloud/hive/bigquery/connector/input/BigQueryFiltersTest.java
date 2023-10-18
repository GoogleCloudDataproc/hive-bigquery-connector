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

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.jupiter.api.Test;

public class BigQueryFiltersTest {

  class CustomUDF extends GenericUDF {
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
      return null;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
      return null;
    }

    @Override
    public String getDisplayString(String[] children) {
      return "customUDF()";
    }
  }

  /**
   * Ensure that, if the user provides a UDF that is not supported, we skip it from BigQuery's
   * pushed down predicates.
   */
  @Test
  public void testSkipCustomUDF() {
    Configuration conf = new Configuration();

    // Define: a = "abcd"
    ExprNodeColumnDesc a = new ExprNodeColumnDesc();
    a.setColumn("a");
    a.setTypeInfo(TypeInfoFactory.stringTypeInfo);
    ExprNodeConstantDesc abcd = new ExprNodeConstantDesc();
    abcd.setValue("abcd");
    abcd.setTypeInfo(TypeInfoFactory.stringTypeInfo);
    ExprNodeGenericFuncDesc equalA = new ExprNodeGenericFuncDesc();
    equalA.setGenericUDF(new GenericUDFOPEqual());
    equalA.setChildren(Arrays.asList(a, abcd));

    // Define: b = "xyz"
    ExprNodeColumnDesc b = new ExprNodeColumnDesc();
    b.setColumn("b");
    b.setTypeInfo(TypeInfoFactory.stringTypeInfo);
    ExprNodeConstantDesc xyz = new ExprNodeConstantDesc();
    xyz.setValue("xyz");
    xyz.setTypeInfo(TypeInfoFactory.stringTypeInfo);
    ExprNodeGenericFuncDesc equalB = new ExprNodeGenericFuncDesc();
    equalB.setGenericUDF(new GenericUDFOPEqual());
    equalB.setChildren(Arrays.asList(b, xyz));

    // Define: CustomUDF()
    ExprNodeGenericFuncDesc customUDF = new ExprNodeGenericFuncDesc();
    customUDF.setGenericUDF(new CustomUDF());

    // Translate: a = "abcd" AND customUDF() AND b = "xyz"
    ExprNodeGenericFuncDesc and = new ExprNodeGenericFuncDesc();
    and.setGenericUDF(new GenericUDFOPAnd());
    and.setChildren(Arrays.asList(equalA, customUDF, equalB));
    assertEquals(
        "((a = 'abcd') and (b = 'xyz'))",
        BigQueryFilters.translateFilters(and, conf).getExprString());

    // Translate: a = "abcd" AND customUDF()
    and = new ExprNodeGenericFuncDesc();
    and.setGenericUDF(new GenericUDFOPAnd());
    and.setChildren(Arrays.asList(equalA, customUDF));
    assertEquals("(a = 'abcd')", BigQueryFilters.translateFilters(and, conf).getExprString());

    // Translate: customUDF() AND customUDF()
    and = new ExprNodeGenericFuncDesc();
    and.setGenericUDF(new GenericUDFOPAnd());
    and.setChildren(Arrays.asList(customUDF, customUDF));
    assertEquals(null, BigQueryFilters.translateFilters(and, conf));

    // Translate: a = "abcd" OR customUDF() OR b = "xyz"
    ExprNodeGenericFuncDesc or = new ExprNodeGenericFuncDesc();
    or.setGenericUDF(new GenericUDFOPOr());
    or.setChildren(Arrays.asList(equalA, customUDF, equalB));
    assertEquals(null, BigQueryFilters.translateFilters(or, conf));
  }
}
