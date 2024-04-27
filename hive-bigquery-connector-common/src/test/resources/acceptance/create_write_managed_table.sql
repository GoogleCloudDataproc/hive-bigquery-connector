-- Create a managed Hive BQ table. 
DROP TABLE IF EXISTS ${HIVE_TEST_TABLE};
CREATE TABLE ${HIVE_TEST_TABLE} (
  number INT,
  text STRING)
STORED BY 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'
TBLPROPERTIES (
  'bq.table'='${BQ_PROJECT}.${BQ_DATASET}.${HIVE_TEST_TABLE}'
);

-- Write data to Hive BQ table.
INSERT INTO ${HIVE_TEST_TABLE} VALUES(123, 'hello');