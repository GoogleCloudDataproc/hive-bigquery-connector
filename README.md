# Hive-BigQuery Connector

The Hive-BigQuery Connector is a Hive StorageHandler that enables Hive to interact with BigQuery's storage layer. It
allows you to keep your existing Hive queries but move data to BigQuery. It utilizes the high throughput
[BigQuery Storage API](https://cloud.google.com/bigquery/docs/reference/storage/) to read and write data.

## Release notes

See the details in [CHANGES.md](CHANGES.md).

## Version support

This connector has been tested with Hive 3.1.2, Hadoop 2.10.1, and Tez 0.9.1.

## Getting the StorageHandler

1. Check it out from GitHub.
2. Build it with the new Google [Hadoop BigQuery Connector](https://cloud.google.com/dataproc/docs/concepts/connectors/bigquery)
``` shell
git clone https://github.com/GoogleCloudPlatform/hive-bigquery-connector
cd hive-bigquery-connector
mvn clean install
```
3. Deploy hive-bigquery-connector-2.0.0-jar-with-dependencies.jar

## Using the StorageHandler to access BigQuery

1. Enable the BigQuery Storage API. Follow [these instructions](https://cloud.google.com/bigquery/docs/reference/storage/#enabling_the_api)
   and check [pricing details](https://cloud.google.com/bigquery/pricing#storage-api)

2. Copy the compiled Jar to a Google Cloud Storage bucket that can be accessed by your hive cluster

3. Open Hive CLI and load the jar as shown below:

```shell
hive> add jar gs://<Jar location>/hive-bigquery-connector-2.0.0-jar-with-dependencies.jar;
```

4. Verify the jar is loaded successfully

```shell
hive> list jars;
```

At this point you can operate Hive just like you used to do.

### Creating BigQuery tables

If you already have a BigQuery table, here is an example of how you can define a Hive table that refers to it:

```sql
CREATE TABLE my_table (word_count bigint, word string)
 STORED BY 'com.google.cloud.hive.bigquery.connector.BigQueryStorageHandler'
 TBLPROPERTIES (
   'bq.dataset'='<BigQuery dataset name>',
   'bq.table'='<BigQuery table name>',
   'bq.project'='<Your Project ID>'
 );
```

The following table properties are required:

| Property     | Description                                                                            |
|--------------|----------------------------------------------------------------------------------------|
| `bq.dataset` | BigQuery dataset name (Optional if the hive database name matches the BQ dataset name) |
| `bq.table`   | BigQuery table name (Optional if hive the table name matches the BQ table name)        |
| `bq.project` | Your project id                                                                        |

### Configuration

| Property                  | Default value       | Description                                                                                                                                                                                        |
|---------------------------|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `bq.write.method`         | `direct`            | Indicates how to write data to BigQuery. Possible values: `direct` (to directly write to the BigQuery storage API), `indirect` (to stage temprary Avro files to GCS before loading into BigQuery). |
| `bq.temp.gcs.path`        |                     | GCS location for storing temporary Avro files when using the `indirect` write method                                                                                                               |
| `bq.work.dir.parent.path` | `${hadoop.tmp.dir}` | Parent path on HDFS where each job creates its temporary work directory                                                                                                                            |
 | `bq.work.dir.name.prefix` | `bq-hive-`          | Prefix used for naming the jobs' temporary directories.                                                                                                                                            |
 | `bq.read.data.format`     | `arrow`             | Data format used for reads from BigQuery. Possible values: `arrow`, `avro`.                                                                                                                        |

### Data Type Mapping

| BigQuery  | Hive      | DESCRIPTION                                                                                                                               |
|-----------|-----------|-------------------------------------------------------------------------------------------------------------------------------------------|
| INTEGER   | BIGINT    | Signed 8-byte Integer                                                                                                                     |
| FLOAT     | DOUBLE    | 8-byte double precision floating point number                                                                                             |
| DATE      | DATE      | FORMAT IS YYYY-[M]M-[D]D. The range of values supported for the Date type is 0001-­01-­01 to 9999-­12-­31                                 |
| TIMESTAMP | TIMESTAMP | Represents an absolute point in time since Unix epoch with millisecond precision (on Hive) compared to Microsecond precision on Bigquery. |
| BOOLEAN   | BOOLEAN   | Boolean values are represented by the keywords TRUE and FALSE                                                                             |
| STRING    | STRING    | Variable-length character data                                                                                                            |
| BYTES     | BINARY    | Variable-length binary data                                                                                                               |
| REPEATED  | ARRAY     | Represents repeated values                                                                                                                |
| RECORD    | STRUCT    | Represents nested structures                                                                                                              |

### Execution engines

The BigQuery storage handler supports both the MapReduce and Tez execution engines. Tez is recommended for better
performance -- you can use it by setting the `hive.execution.engine=tez` configuration property.

### Filtering

The new API allows column pruning and predicate filtering to only read the data you are interested in.

#### Column Pruning

Since BigQuery is [backed by a columnar datastore](https://cloud.google.com/blog/big-data/2016/04/inside-capacitor-bigquerys-next-generation-columnar-storage-format),
it can efficiently stream data without reading all columns.

Column pruning is currently supported only with the Tez engine.

#### Predicate Filtering

The Storage API supports arbitrary pushdown of predicate filters. To enable predicate pushdown ensure
`hive.optimize.ppd` is set to `true`.  Filters on all primitive type columns will be pushed to storage layer improving
the performance of reads. Predicate pushdown is not supported on complex types such as arrays and structs.
For example - filters like `address.city = "Sunnyvale"` will not get pushdown to Bigquery.

### Known issues and limitations

1. The `TINYINT`, `SMALLINT`, and `INT`/`INTEGER` Hive types are not supported. Instead, use the `BIGINT` type, which
   corresponds to BigQuery's `INT64` type. Similarly, the `FLOAT` Hive type is not supported. Instead, use the `DOUBLE`
   type, which corresponds to BigQuery's `FLOAT64` type.
2. Ensure that the table exists in BigQuery and column names are always lowercase.
3. A `TIMESTAMP` column in hive is interpreted to be timezone-less and stored as an offset from the UNIX epoch with
   milliseconds precision. To display in human-readable format, use the `from_unix_time` UDF:

   ```sql
   from_unixtime(cast(cast(<timestampcolumn> as bigint)/1000 as bigint), 'yyyy-MM-dd hh:mm:ss')
   ```
4. If a write job fails while using the Tez execution engine and the `indirect` write method, then the temporary avro
   files might not be automatically cleaned up from the GCS bucket. The MR execution engine does not have that
   limitation. The temporary files are always cleaned up when the job is successful, regardless of the execution engine
   in use.

# Development

## Code formatting

To standardize the code's format, run [Spotless](https://github.com/diffplug/spotless) like so:

```sh
mvn spotless:apply
```

## Running the tests

You must use Java version 8, as it's the version that Hive itself uses. Make sure that `JAVA_HOME` points to the Java
8's base directory.

Create a service account and give the following roles in your project:

- BigQuery Data Owner
- BigQuery Job User
- BigQuery Read Session User
- Storage Admin

Download a JSON private key for the service account, and set the `GOOGLE_APPLICATION_CREDENTIALS` environment
variable:

```sh
GOOGLE_APPLICATION_CREDENTIALS=<path/to/your/key.json>
```

To run the integration tests:

```sh
mvn verify --projects shaded-dependencies,connector -Dit.test="IntegrationTests"
```

To run a specific test method:

```sh
mvn verify --projects shaded-dependencies,connector -Dit.test="IntegrationTests#testInsertTez"
```

To debug the tests, add the `-Dmaven.failsafe.debug` property:

```sh
mvn verify -Dmaven.failsafe.debug --projects shaded-dependencies,connector -Dit.test="IntegrationTests"
```

... then run a remote debugger in IntelliJ at port 5005.

Read more about debugging with FailSafe here: https://maven.apache.org/surefire/maven-failsafe-plugin/examples/debugging.html
