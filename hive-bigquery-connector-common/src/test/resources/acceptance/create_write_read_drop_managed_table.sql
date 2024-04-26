-- Create a managed Hive BQ table. 
DROP TABLE IF EXISTS ${HIVE_TEST_TABLE};
CREATE TABLE ${HIVE_TEST_TABLE} (
  id INT,
  name STRING)
STORED BY 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'
TBLPROPERTIES (
  'bq.table'='${BQ_PROJECT}.${BQ_DATASET}.${HIVE_TEST_TABLE}'
);

-- Write data to Hive BQ table.
INSERT INTO ${HIVE_TEST_TABLE} VALUES(123, 'hello');
INSERT INTO ${HIVE_TEST_TABLE} VALUES(345, 'world');

-- Read Hive BQ table, write the result into an output table backed by GCS.
CREATE TABLE ${HIVE_OUTPUT_TABLE}
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  LOCATION '${HIVE_OUTPUT_DIR_URI}'
AS SELECT * from ${HIVE_TEST_TABLE} WHERE id = 345;

-- Drop the managed table
DROP TABLE ${HIVE_TEST_TABLE};
