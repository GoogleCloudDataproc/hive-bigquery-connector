package com.google.cloud.hive.bigquery.connector.input.udfs;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/** Converts Hive's modulo operator operator to BigQuery's mod() function. */
public class BigQueryUDFRLike extends GenericUDF {

    public BigQueryUDFRLike() {}

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // Ignore
        return null;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        // Ignore
        return null;
    }

    @Override
    public String getDisplayString(String[] children) {
        return String.format("REGEXP_CONTAINS(%s,r%s)", children[0], children[1]);
    }
}

