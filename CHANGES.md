# Release notes

## v2.0.0

- Added support for reads in the Arrow format (now the new default).
- Added support for direct writes to the BigQuery Storage API (now the new default).
- Added full support for the Tez execution engine.
- Added support for `INSERT OVERWRITE` statements.
- There is no longer a requirement to supply an explicit avro schema for `ARRAY` and `STRUCT` types.

## v1.0.0

- Support for reads in the Avro format.
- Support for writes threw temporary Avro files staged on GCS.