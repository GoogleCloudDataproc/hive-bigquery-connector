# Hive-BigQuery Connector

The Hive-BigQuery Connector is a Hive storage handler that enables Hive to interact with BigQuery's
storage layer. It allows you to run queries in Hive using the HiveQL dialect to read from and write
to BigQuery.

## Release notes

See the details in [CHANGES.md](CHANGES.md).

## Version support

This connector supports [Dataproc](https://cloud.google.com/dataproc) 2.0 and 2.1.

For Hadoop clusters other than Dataproc, the connector has been tested with the following
software versions:

* Hive 3.1.2 and 3.1.3.
* Hadoop 2.10.2, 3.2.3 and 3.3.3.
* Tez 0.9.2 on Hadoop 2, and Tez 0.10.1 on Hadoop 3.

## Installation

1. Enable the BigQuery Storage API. Follow [these instructions](https://cloud.google.com/bigquery/docs/reference/storage/#enabling_the_api).

2. Clone this repository:
   ```sh
   git clone https://github.com/GoogleCloudPlatform/hive-bigquery-connector
   cd hive-bigquery-connector
   ```

3. Compile and package the JAR:
   ``` sh
   ./mvnw package -DskipTests
   ```
   The packaged JAR is now available at: `connector/target/hive-bigquery-connector-2.0.0-SNAPSHOT.jar`

4. Upload the JAR to your cluster.

5. Pass the JAR's path with the `--auxpath` parameter when you start your Hive session:

   ```sh
   hive --auxpath <JAR path>/hive-bigquery-connector-2.0.0-SNAPSHOT.jar
   ```

## Managed tables vs external tables

Hive can have [two types](https://cwiki.apache.org/confluence/display/Hive/Managed+vs.+External+Tables)
of tables:

- Managed tables, sometimes referred to as internal tables.
- External tables.

The Hive BigQuery connector supports both types in the following ways.

### Managed tables

When you create a managed table using the `CREATE TABLE` statement, the connector creates both
the table metadata in the Hive Metastore and a new table in BigQuery with the same schema.

Here's an example:

```sql
CREATE TABLE mytable (word_count BIGINT, word STRING)
STORED BY 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'
TBLPROPERTIES (
    'bq.table'='myproject.mydataset.mytable'
);
```

When you drop a managed table using the `DROP TABLE` statement, the connector drops both the table
metadata from the Hive Metastore and the BigQuery table (including all of its data).

For Hive-3.x, create a managed table with `NOT NULL` column restraint will not create the BigQuery table
with corresponding `NOT NULL` restraint. The `NOT NULL` restraint is still enforced by Hive at runtime.
It is recommended to use external table if user needs to have the `NOT NULL` restraint on the BigQuery table.

### External tables

When you create an external table using the `CREATE EXTERNAL TABLE` statement, the connector only
creates the table metadata in the Hive Metastore. It assumes that the corresponding table in
BigQuery already exists.

Here's an example:

```sql
CREATE EXTERNAL TABLE mytable (word_count BIGINT, word STRING)
STORED BY 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'
TBLPROPERTIES (
    'bq.table'='myproject.mydataset.mytable'
);
```

When you drop an external table using the `DROP TABLE` statement, the connector only drops the table
metadata from the Hive Metastore. The corresponding BigQuery table remains unaffected.

### Statistics For Hive Query Planning

It is recommended to collect [statistics](https://cwiki.apache.org/confluence/display/hive/statsdev)
(e.g. the number of rows, raw data size, etc) to help Hive to optimize query plan,
therefore improving performance. Follow these steps to collect statistics for a table:
(replace `<table_name>` with your table name)

If your Hive has [HIVE-24928](https://issues.apache.org/jira/browse/HIVE-24928) (this is applied on Dataproc),
statistics can be collected by a quick metadata collection operation through
```sql
ANALYZE TABLE <table_name> COMPUTE STATISTICS;
```
To verify if Hive has reasonable statistics on `numRows` and `rawDataSize` for your table.
   ```sql
   DESCRIBE FORMATTED <table_name>;
   ```
Example output:
   ```
   Table Parameters:
       COLUMN_STATS_ACCURATE	{\"BASIC_STATS\":\"true\"}
       numFiles            	0
       numRows             	12345
       rawDataSize         	67890
       totalSize           	67890
   ```

Note: Without [HIVE-24928](https://issues.apache.org/jira/browse/HIVE-24928), user can run
```sql
ANALYZE TABLE <table_name> COMPUTE STATISTICS FOR COLUMNS;
```
to collect column stats and run query with the assistance of column stats.
```sql
SET hive.stats.fetch.column.stats=true;
```
Column stats collection is more expensive operation, so it is recommended to have [HIVE-24928](https://issues.apache.org/jira/browse/HIVE-24928) applied.

## Partitioning

As Hive's partitioning and BigQuery's partitioning inherently work in different ways, the Hive
`PARTITIONED BY` clause is not supported. However, you can still leverage BigQuery's partitioning by
using table properties. Two types of partitioning are currently supported: time-unit column
partitioning and ingestion time partitioning.

### Time-unit column partitioning

You can partition a table on a column of Hive type `DATE`, `TIMESTAMP`, or `TIMESTAMPLOCALTZ`, which
respectively correspond to the BigQuery `DATE`, `DATETIME`, and `TIMESTAMP` types. When you write
data to the table, BigQuery automatically puts the data into the correct partition based on the
values in the column.

For `TIMESTAMP` and `TIMESTAMPLOCALTZ` columns, the partitions can have either hourly, daily,
monthly, or yearly granularity. For `DATE` columns, the partitions can have daily, monthly, or
yearly granularity. Partitions boundaries are based on UTC time.

To create a table partitioned by a time-unit column, you must set the `bq.time.partition.field`
table property to the column's name.

Here's an example:

```sql
CREATE TABLE mytable (int_val BIGINT, ts TIMESTAMP)
STORED BY 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'
TBLPROPERTIES (
    'bq.table'='myproject.mydataset.mytable',
    'bq.time.partition.field'='ts',
    'bq.time.partition.type'='MONTH'
);
```

Check out the official BigQuery documentation about [Time-unit column partitioning](https://cloud.google.com/bigquery/docs/partitioned-tables#date_timestamp_partitioned_tables)
to learn more.

### Ingestion time partitioning

When you create a table partitioned by ingestion time, BigQuery automatically assigns rows to
partitions based on the time when BigQuery ingests the data. You can choose hourly, daily, monthly,
or yearly boundaries for the partitions. Partitions boundaries are based on UTC time.

An ingestion-time partitioned table also has two pseudo columns:

- `_PARTITIONTIME`: ingestion time for each row, truncated to the partition boundary (such as hourly
  or daily). This column has the `DATE` Hive type, which corresponds to the BigQuery `DATE` type.
- `_PARTITIONDATE`: UTC date corresponding to the value in the `_PARTITIONTIME` pseudo column. This
  column has the `TIMESTAMPLOCALTZ` Hive type, which corresponds to the BigQuery `TIMESTAMP` type.

To create a table partitioned by ingestion time, you must set the `bq.time.partition.type` table
property to the partition boundary of your choice (`HOUR`, `DAY`, `MONTH`, or `YEAR`).

Here's an example:

```sql
CREATE TABLE mytable (int_val BIGINT)
STORED BY 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'
TBLPROPERTIES (
    'bq.table'='myproject.mydataset.mytable',
    'bq.time.partition.type'='DAY'
);
```

Note: Ingestion time partitioning is currently supported only for read operations.

Check out the official BigQuery documentation about [Ingestion time partitioning](https://cloud.google.com/bigquery/docs/partitioned-tables#ingestion_time)
to learn more.

## Clustering

As Hive's clustering and BigQuery's clustering inherently work in different ways, the Hive
`CLUSTERED BY` clause is not supported. However, you can still leverage BigQuery's clustering by
setting the `bq.clustered.fields` table property to a comma-separated list of the columns to cluster
the table by.

Here's an example:

```sql
CREATE TABLE mytable (int_val BIGINT, text STRING, purchase_date DATE)
STORED BY 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'
TBLPROPERTIES (
    'bq.table'='myproject.mydataset.mytable',
    'bq.clustered.fields'='int_val,text'
);
```

Check out the official BigQuery documentation about [Clustering](https://cloud.google.com/bigquery/docs/clustered-tables)
to learn more.

## Tables properties

You can use the following properties in the `TBLPROPERTIES` clause when you create a new table:

| Property                           | Description                                                                                                                                                                                                                                                       |
|------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `bq.table`                         | Always required. BigQuery table name in the format of project.dataset.table                                                                                                                                                                                       |
| `bq.time.partition.type`           | Time partitioning granularity. Possible values: `HOUR`, `DAY`, `MONTH`, `YEAR`                                                                                                                                                                                    |
| `bq.time.partition.field`          | Name of a `DATE` or `TIMESTAMP` column to partition the table by                                                                                                                                                                                                  |
| `bq.time.partition.expiration.ms`  | Partition [expiration time](https://cloud.google.com/bigquery/docs/managing-partitioned-tables#partition-expiration) in milliseconds                                                                                                                              |
| `bq.time.partition.require.filter` | Set it to `true` to require that all queries on the table [must include a predicate filter]((https://cloud.google.com/bigquery/docs/managing-partitioned-tables#require-filter)) (a `WHERE` clause) that filters on the partitioning column. Defaults to `false`. |
| `bq.clustered.fields`              | Comma-separated list of fields to cluster the table by                                                                                                                                                                                                            |                                                                                                                                                                                                 |

## Job configuration properties

You can set the following Hive/Hadoop configuration properties in your environment:

| Property                  | Default value       | Description                                                                                                                                                                                        |
|---------------------------|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `bq.read.data.format`     | `arrow`             | Data format used for reads from BigQuery. Possible values: `arrow`, `avro`.                                                                                                                        |
| `bq.temp.gcs.path`        |                     | GCS location for storing temporary Avro files when using the `indirect` write method                                                                                                               |
| `bq.write.method`         | `direct`            | Indicates how to write data to BigQuery. Possible values: `direct` (to directly write to the BigQuery storage API), `indirect` (to stage temprary Avro files to GCS before loading into BigQuery). |
| `bq.work.dir.parent.path` | `${hadoop.tmp.dir}` | Parent path on HDFS where each job creates its temporary work directory                                                                                                                            |
| `bq.work.dir.name.prefix` | `bq-hive-`          | Prefix used for naming the jobs' temporary directories.                                                                                                                                            |
| `materializationProject`  |                     | Project used to temporarily materialize data when reading views. Defaults to the same project as the read view.                                                                                    |
| `materializationDataset`  |                     | Dataset used to temporarily materialize data when reading views. Defaults to the same dataset as the read view.                                                                                    |
| `maxParallelism`          |                     | Maximum initial number of read streams                                                                                                                                                             |
| `viewsEnabled`            | `false`             | Set it to `true` to enable reading views.                                                                                                                                                          |

## Data Type Mapping

Add links to Hive & BQ types doc.

| Hive               | Hive type description                                                                             | BigQuery        | BigQuery type description                                                                                                                        |
|--------------------|---------------------------------------------------------------------------------------------------|-----------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| `TINYINT`          | 1-byte signed integer                                                                             | `INT64`         |                                                                                                                                                  |
| `SMALLINT`         | 2-byte signed integer                                                                             | `INT64`         |                                                                                                                                                  |
| `INT`              | 4-byte signed integer                                                                             | `INT64`         |                                                                                                                                                  |
| `BIGINT`           | 8-byte signed integer                                                                             | `INT64`         |                                                                                                                                                  |
| `FLOAT`            | 4-byte single precision floating point number                                                     | `FLOAT64`       |                                                                                                                                                  |
| `DOUBLE`           | 8-byte double precision floating point number                                                     | `FLOAT64`       |                                                                                                                                                  |
| `DECIMAL`          | Alias of `NUMERIC`. Precision: 38. Scale: 38                                                      | `DECIMAL`       | Alias of `NUMERIC`. Precision: 38. Scale: 9.                                                                                                     |
| `DATE`             | Format: `YYYY-MM-DD`                                                                              | `DATE`          | Format: `YYYY-[M]M-[D]D`. Supported range: 0001-01-01 to 9999-12-31                                                                              |
| `TIMESTAMP`        | Timezone-less timestamp stored as an offset from the UNIX epoch                                   | `DATETIME`      | A date and time, as they might be displayed on a watch, independent of time zone.                                                                |
| `TIMESTAMPLOCALTZ` | Timezoned timestamp stored as an offset from the UNIX epoch                                       | `TIMESTAMP`     | Absolute point in time, independent of any time zone or convention such as Daylight Savings Time                                                 |
| `BOOLEAN`          | Boolean values are represented by the keywords TRUE and FALSE                                     | `BOOLEAN`       |                                                                                                                                                  |
| `CHAR`             | Variable-length character data                                                                    | `STRING`        |                                                                                                                                                  |
| `VARCHAR`          | Variable-length character data                                                                    | `STRING`        |                                                                                                                                                  |
| `STRING`           | Variable-length character data                                                                    | `STRING`        |                                                                                                                                                  |
| `BINARY`           | Variable-length binary data                                                                       | `BYTES`         |                                                                                                                                                  |
| `ARRAY`            | Represents repeated values                                                                        | `ARRAY`         |                                                                                                                                                  |
| `STRUCT`           | Represents nested structures                                                                      | `STRUCT`        |                                                                                                                                                  |
| `MAP`              | Dictionary of keys and values. Keys must be of primitive type, whereas values can be of any type. | `ARRAY<STRUCT>` | BigQuery doesn't support Maps natively. The connector implements it as a list of structs, where each struct has two columns: `name` and `value`. |

## Execution engines

The BigQuery storage handler supports both the MapReduce and Tez execution engines. Tez is recommended for better
performance -- you can use it by setting the `hive.execution.engine=tez` configuration property.

## Column Pruning

Since BigQuery is [backed by a columnar datastore](https://cloud.google.com/blog/big-data/2016/04/inside-capacitor-bigquerys-next-generation-columnar-storage-format),
it can efficiently stream data without reading all columns.

## Predicate pushdowns

The connector supports predicate pushdowns to the BigQuery Storage Read API. This allows to filter
data at the BigQuery storage layer, which reduces the amount of data traversing the network and
improves overall performance.

Many built-in Hive UDFs and operators (e.g. `AND`, `OR`, `ABS`, `TRIM`, `BETWEEN`...) are identical
in BigQuery, so the connector passes those as-is to BigQuery.

However, some Hive UDFs and operators are different in BigQuery. So the connector automatically
converts those to the equivalent functions in BigQuery. Below is the list of UDFs and operators that
are automatically converted:

| Hive generic UDF | BigQuery function             | Notes                                                                                     |
|------------------|-------------------------------|-------------------------------------------------------------------------------------------|
| `%`              | `MOD`                         | BigQuery currently supports `MOD` only for the `INT64`, `NUMERIC`, and `BIGNUMERIC` types |
| `TO_DATE`        | `DATE`                        |                                                                                           |
| `DATE_ADD`       | `DATE_ADD`                    |                                                                                           |
| `DATE_SUB`       | `DATE_SUB`                    |                                                                                           |
| `DATEDIFF`       | `DATE_DIFF`                   |                                                                                           |
| `DATEDIFF`       | `DATE_DIFF`                   |                                                                                           |
| `HEX`            | `TO_HEX`                      |                                                                                           |
| `UNHEX`          | `FROM_HEX`                    |                                                                                           |
| `NVL`            | `IFNULL`                      |                                                                                           |
| `RLIKE`          | `REGEXP_CONTAINS`             |                                                                                           |
| `SHIFTLEFT`      | `<<`                          |                                                                                           |
| `SHIFTRIGHT`     | `>>`                          |                                                                                           |
| `YEAR`           | `EXTRACT(YEAR FROM ...)`      |                                                                                           |
| `MONTH`          | `EXTRACT(MONTH FROM ...)`     |                                                                                           |
| `DAY`            | `EXTRACT(DAY FROM ...)`       |                                                                                           |
| `HOUR`           | `EXTRACT(HOUR FROM ...)`      |                                                                                           |
| `MINUTE`         | `EXTRACT(MINUTE FROM ...)`    |                                                                                           |
| `SECOND`         | `EXTRACT(SECOND FROM ...)`    |                                                                                           |
| `DAYOFWEEK`      | `EXTRACT(DAYOFWEEK FROM ...)` |                                                                                           |
| `WEEKOFYEAR`     | `EXTRACT(WEEK FROM ...)`      |                                                                                           |
| `QUARTER`        | `EXTRACT(QUARTER FROM ...)`   |                                                                                           |

Note: [Custom Hive UDFs](https://cwiki.apache.org/confluence/display/hive/hiveplugins) (aka Hive plugins) are currently not supported.

## Parallelism

### Parallel reads

The connector allows parallel reads from BigQuery by using the
[BigQuery Storage API](https://cloud.google.com/bigquery/docs/reference/storage).

You can set the `preferredMinParallelism` configuration property, which the connector passes to
[`CreateReadSessionRequest.setPreferredMinStreamCount()`](https://cloud.google.com/java/docs/reference/google-cloud-bigquerystorage/latest/com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest.Builder#com_google_cloud_bigquery_storage_v1_CreateReadSessionRequest_Builder_setPreferredMinStreamCount_int_)
when it creates the BigQuery read session. This parameter can be used to inform the BigQuery service that there is a
desired lower bound on the number of streams. This is typically the target parallelism of the client (e.g. a Hive
cluster with N-workers would set this to a low multiple of N to ensure good cluster utilization). The BigQuery backend
makes a best effort to provide at least this number of streams, but in some cases might provide less.

Additionally, you can set the `maxParallelism` configuration property, which the connector passes to
[CreateReadSessionRequest.setMaxStreamCount()](https://cloud.google.com/java/docs/reference/google-cloud-bigquerystorage/latest/com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest.Builder#com_google_cloud_bigquery_storage_v1_CreateReadSessionRequest_Builder_setMaxStreamCount_int_).
If unset or zero, the BigQuery backend server will provide a value of streams to produce reasonable throughput. The
number of streams may be lower than the requested number, depending on the amount parallelism that is reasonable for
the table. There is a default system max limit of 1,000. This must be greater than or equal to the number of MapReduce
splits. Typically, a client should either leave this unset to let the system determine an upper bound or set this as the
maximum "units of work" that the client can gracefully handle.

The connector supports both the [Arrow](https://cloud.google.com/bigquery/docs/reference/storage#arrow_schema_details)
and [Avro](https://cloud.google.com/bigquery/docs/reference/storage#avro_schema_details) read formats. You can specify
which format the connector should use by setting the `bq.read.data.format` configuration property to either `arrow` or
`avro`. The connector uses Arrow by default as Arrow generally performs better than Avro.

### Parallel writes

The connector supports two methods for writing to BigQuery: direct writes and indirect writes.

#### Direct write method

The direct write method consists of writing directly to BigQuery by using the [BigQuery Write API in "pending" mode](https://cloud.google.com/bigquery/docs/write-api-batch).

The indirect write method consists of the following steps:

- During the execution of a write job, each mapper task creates its own BigQuery write stream and writes directly to
  BigQuery in parallel.
- At the end of the job, the connector commits all the streams together atomically. If the commit succeeds, then the
  newly written data instantly becomes available for reading.

If for some reason the job fails, all the writes that may have been done through the various open streams are
automatically garbage-collected by the BigQuery backend and none of the data ends up persisting in the target table.

The direct write method is generally faster than the indirect write method but incurs [costs](https://cloud.google.com/bigquery/pricing#data_ingestion_pricing)
associated with usage of the BigQuery Storage Write API.

The connector uses this method by default.

#### Indirect write method

The indirect write method consists of the following steps:

- During the execution of a write job, each mapper task creates its own temporary output Avro file and stages it to GCS.
- At the end of the job, the connector commits the writes by executing a [BigQuery load job](https://cloud.google.com/bigquery/docs/batch-loading-data)
  with all the Avro files.
- Once the job is complete, the connector deletes the temporary Avro files from GCS.

This method is generally costs less than the direct write method as [BigQuery load jobs are free](https://cloud.google.com/bigquery/pricing#data_ingestion_pricing)
and this method only incur [costs related to GCS write operations and storage](https://cloud.google.com/storage/pricing).
However, this method is also generally much slower due to its multi-stage nature and data being routed through GCS.
Learn more about other [limitations](https://cloud.google.com/bigquery/docs/batch-loading-data#limitations).

The connector uses the direct write method by default. To let it use the indirect method instead,
set the `bq.write.method` configuration property to `indirect`, and set the `bq.temp.gcs.path`
property to indicate where to store the temporary Avro files in GCS.

## Reading From BigQuery Views

The connector has preliminary support for reading from [BigQuery views](https://cloud.google.com/bigquery/docs/views-intro).
Please note there are a few caveats:

* The Storage Read API operates on storage directly, so the API cannot be used to read logical or materialized views. To
  get around this limitation, the connector materializes the views into temporary tables before it can read them. This
  materialization process can affect overall read performance and incur additional costs to your BigQuery bill.
* By default, the materialized views are created in the same project and dataset. Those can be configured by the
  optional `materializationProject` and `materializationDataset` Hive configuration properties or
  table properties, respectively.
* As mentioned in the [BigQuery documentation](https://cloud.google.com/bigquery/docs/writing-results#temporary_and_permanent_tables),
  the `materializationDataset` should be in same location as the view.
* Reading from views is **disabled** by default. In order to enable it, set the `viewsEnabled` configuration
  property to `true`.

## Known issues and limitations

* The `UPDATE`, `MERGE`, and `DELETE`, and `ALTER TABLE` statements are currently not supported.
* The `PARTITIONED BY`, `CLUSTERED BY`, `INSERT INTO PARTITION`, and
  `INSERT INTO PARTITION OVERWRITE` statements are currently not supported. Note, however, that
  partitioning and clustering in BigQuery are supported via `TBLPROPERTIES`. See the corresponding
  sections on [partitioning](#partitioning) and [clustering](#clustering).
* CTAS (aka `CREATE TABLE AS SELECT`) and CTLT (`CREATE TABLE LIKE TABLE`) statements are currently
  not supported.
* If a write job fails when using the Tez execution engine and the `indirect` write method, the
  temporary avro files might not be automatically cleaned up from the GCS bucket. The MR execution
  engine does not have this limitation. The temporary files are always cleaned up when the job is
  successful, regardless of the execution engine in use.
* If you use the Hive `MAP` type, then the map's key must be of `STRING` type if you use the Avro
  format for reading or the indirect method for writing. This is because Avro requires keys to be
  strings. If you use the Arrow format for reading (default) and the direct method for writing (also
  default), then there are no type limitations for the keys.
* Hive `DECIMAL` data type has precision of 38 and scale of 38, while BigQuery's `NUMERIC`/`BIGNUMERIC` have different precisions and scales.
  (https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types),
  If the BigQuery data of `BIGNUMERIC`'s precision and scale out of Hive's `DECIMAL` range, data can show up as NULL.
* [Custom Hive UDFs](https://cwiki.apache.org/confluence/display/hive/hiveplugins) (aka Hive plugins) are currently not supported.
* BigQuery [ingestion time partitioning](https://cloud.google.com/bigquery/docs/partitioned-tables#ingestion_time) is currently supported only for read operations.
* BigQuery [integer range partitioning](https://cloud.google.com/bigquery/docs/partitioned-tables#integer_range) is currently not supported.

## Development

### Code formatting

To standardize the code's format, use [Spotless](https://github.com/diffplug/spotless):

```sh
./mvnw spotless:apply
```

### Unit/integration tests

#### Set up IAM permissions

Create a service account and give the following roles in your project:

- BigQuery Admin
- Storage Admin

Download a JSON private key for the service account, and set the `GOOGLE_APPLICATION_CREDENTIALS` environment
variable:

```sh
export GOOGLE_APPLICATION_CREDENTIALS=<path/to/your/key.json>
```

#### Enable APIs

Enable the following APIs:

```sh
gcloud services enable \
  bigquerystorage.googleapis.com \
  bigqueryconnection.googleapis.com
```

#### BigLake setup

Define environment variables:

```sh
export PROJECT=my-gcp-project
export BIGLAKE_LOCATION=us
export BIGLAKE_REGION=us-central1
export BIGLAKE_CONNECTION=hive-integration-tests
export BIGLAKE_BUCKET=${USER}-biglake-test
```

Create the test BigLake connection:

```sh
bq mk \
  --connection \
  --project_id="${PROJECT}" \
  --location="${BIGLAKE_LOCATION}" \
  --connection_type=CLOUD_RESOURCE \
  "${BIGLAKE_CONNECTION}"
```

Create the bucket to host BigLake datasets:

```sh
gsutil mb -l "${BIGLAKE_REGION}" "gs://${BIGLAKE_BUCKET}"
```

Give the BigLake connection's service account access to the bucket:

```sh
export BIGLAKE_SA=$(bq show --connection --format json "${PROJECT}.${BIGLAKE_LOCATION}.${BIGLAKE_CONNECTION}" \
  | jq -r .cloudResource.serviceAccountId)

gsutil iam ch serviceAccount:${BIGLAKE_SA}:objectViewer gs://${BIGLAKE_BUCKET}
```

#### Running the tests

You must use Java version 8, as it's the version that Hive itself uses. Make sure that `JAVA_HOME` points to the Java
8's base directory.

##### Integration tests

* To run the integration tests:
  ```sh
  ./mvnw verify -Pdataproc21,integration
  ```

* To run a single integration test class:
  ```sh
  ./mvnw verify -Pdataproc21,integration -Dit.test="BigLakeIntegrationTests"
  ```

* To run a specific integration test method:
  ```sh
  ./mvnw verify -Pdataproc21,integration -Dit.test="BigLakeIntegrationTests#testReadBigLakeTable"
  ```

* To debug the tests, add the `-Dmaven.failsafe.debug` property:
  ```sh
  ./mvnw verify -Pdataproc21,integration -Dmaven.failsafe.debug
  ```
  ... then run a remote debugger in IntelliJ at port `5005`. Read more about debugging with FailSafe
  here: https://maven.apache.org/surefire/maven-failsafe-plugin/examples/debugging.html

##### Acceptance tests

Acceptance tests create Dataproc clusters with the connector and run jobs to verify it.

The following environment variables must be set and **exported** first.

* `GOOGLE_APPLICATION_CREDENTIALS` - the full path to a credentials JSON, either a service account or the result of a
  `gcloud auth login` run
* `GOOGLE_CLOUD_PROJECT` - The Google cloud platform project used to test the connector
* `TEST_BUCKET` - The GCS bucked used to test writing to BigQuery during the integration tests
* `ACCEPTANCE_TEST_BUCKET` - The GCS bucked used to test writing to BigQuery during the acceptance tests

To run the acceptance tests:

```sh
./mvnw verify -Pdataproc21,acceptance
```

If you want to avoid rebuilding `shaded-dependencies` and `shaded-test-dependencies` when there is no changes in these
modules, you can break it down into several steps, and only rerun the necessary steps:

```sh
# Install hive-bigquery-parent/pom.xml to Maven local repo
mvn install:install-file -Dpackaging=pom -Dfile=hive-bigquery-parent/pom.xml -DpomFile=hive-bigquery-parent/pom.xml

# Build and install shaded-dependencies and shaded-test-dependencies jars to Maven local repo
mvn clean install -pl shaded-dependencies,shaded-test-dependencies -Pdataproc21 -DskipTests

# Build and test connector
mvn clean verify -pl connector -Pdataproc21,acceptance
```

##### Running the tests for different Hadoop versions

To run the tests for Hadoop 2, pass the `-Phadoop2` parameter to the `mvnw verify` command to
activate the `hadoop2` Maven profile. For Hadoop 3, pass `-Phadoop3` instead.

Before you can run the tests with Hadoop 3, you also must install Tez's latest (unreleased) 0.9.3:

* Install Protobuf v2.5.0:

  If you're on MacOS, install these packages:

  ```sh
  brew install automake libtool wget
  ```

  Then compile Protobuf from source:

  ```sh
  cd ~
  wget https://github.com/google/protobuf/releases/download/v2.5.0/protobuf-2.5.0.tar.bz2
  tar -xvjf protobuf-2.5.0.tar.bz2
  rm protobuf-2.5.0.tar.bz2
  cd protobuf-2.5.0
  ./autogen.sh
  ./configure --prefix=$(PWD)
  make; make check
  make install
  ```

* Get the Tez source:

  ```sh
  cd ~
  git clone
  cd git@github.com:apache/tez.git
  cd tez
  git checkout origin/branch-0.9
  ```

* Compile and install Tez:

  ```sh
  export PATH=${HOME}/protobuf-2.5.0/bin:${PATH}
  mvn clean install \
    --projects=tez-api,tez-common,tez-mapreduce,tez-dag,hadoop-shim,tez-runtime-library,tez-runtime-internals \
    -DskipTests=true -Dmaven.javadoc.skip=true \
    -Dprotoc.path=${HOME}/protobuf-2.5.0/bin/protoc -Dhadoop.version=3.2.3
  ```

  If all steps have succeeded, then Tez's `0.9.2-SNAPSHOT` packages should be installed in your
  local Maven repository and you should be able to run the tests with Hadoop 3 by using the
  `-Phadoop3` argument.
