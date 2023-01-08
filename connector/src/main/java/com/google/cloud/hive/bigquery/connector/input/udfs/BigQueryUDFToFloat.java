package com.google.cloud.hive.bigquery.connector.input.udfs;

public class BigQueryUDFToFloat extends BigQueryUDFBase{
    public BigQueryUDFToFloat() {}

    @Override
    public String getDisplayString(String[] children) {
        return String.format("CAST(%s as FLOAT64)", children[0]);
    }
}
