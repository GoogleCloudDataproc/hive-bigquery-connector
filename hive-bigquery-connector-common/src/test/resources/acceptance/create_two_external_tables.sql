DROP TABLE IF EXISTS ${HIVE_TEST_TABLE};
CREATE EXTERNAL TABLE ${HIVE_TEST_TABLE} (
  number INT,
  text STRING)
STORED BY 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'
TBLPROPERTIES (
  'bq.table'='${BQ_PROJECT}.${BQ_DATASET}.${HIVE_TEST_TABLE}'
);

DROP TABLE IF EXISTS ${HIVE_OUTPUT_TABLE};
CREATE EXTERNAL TABLE ${HIVE_OUTPUT_TABLE} (
  number INT,
  text STRING)
STORED BY 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'
TBLPROPERTIES (
  'bq.table'='${BQ_PROJECT}.${BQ_DATASET}.${HIVE_OUTPUT_TABLE}'
);