-- Create an external table backed by a shakespeare public BQ table. 
DROP TABLE IF EXISTS shakespeare;

CREATE EXTERNAL TABLE shakespeare (
    word STRING,
    word_count BIGINT,
    corpus STRING,
    corpus_date BIGINT)
STORED BY 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'
TBLPROPERTIES (
    'bq.table'='bigquery-public-data.samples.shakespeare'
);

-- Perform word count, write the result to the output table.
CREATE TABLE ${HIVE_OUTPUT_TABLE}
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LOCATION '${HIVE_OUTPUT_DIR_URI}'
AS
    SELECT word, sum(word_count) as count
    FROM shakespeare
    WHERE word = 'king'
    GROUP BY word;

-- Drop the external table
DROP TABLE shakespeare;
