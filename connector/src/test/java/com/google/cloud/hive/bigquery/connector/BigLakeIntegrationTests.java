package com.google.cloud.hive.bigquery.connector;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.google.cloud.hive.bigquery.connector.TestUtils.*;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import com.google.cloud.hive.bigquery.connector.config.HiveBigQueryConfig;
import org.junitpioneer.jupiter.cartesian.CartesianTest;

public class BigLakeIntegrationTests extends IntegrationTestsBase {

    @CartesianTest
    public void testReadBigLakeTable(
        @CartesianTest.Values(strings = {"mr", "tez"}) String engine,
        @CartesianTest.Values(strings = {HiveBigQueryConfig.ARROW, HiveBigQueryConfig.AVRO})
            String readDataFormat) {
        // Create BigLake table
        runBqQuery(BIGQUERY_BIGLAKE_TABLE_CREATE_QUERY);
        // Create Hive table
        initHive(engine, readDataFormat);
        runHiveScript(HIVE_BIGLAKE_TABLE_CREATE_QUERY);
        // Read data
        List<Object[]> rows = runHiveStatement(String.format("SELECT * FROM %s", BIGLAKE_TABLE_NAME));
        assertArrayEquals(
            new Object[] {
                new Object[] {1L, 2L, 3L},
                new Object[] {4L, 5L, 6L},
            },
            rows.toArray());
    }

}
