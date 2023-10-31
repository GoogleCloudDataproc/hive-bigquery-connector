# Release Notes

## Next

* Added support for Hive 2.X.
* Added support for Spark SQL.
* Fixed case sensitivity bug with column names. This particularly affected pseudo columns like
  `_PARTITIONTIME` and `_PARTITIONDATE` in time-ingestion partitioned BigQuery tables.
* **Backward-incompatible change:** The type of the `_PARTITION_TIME` pseudo-column in
  time-ingestion partitioned tables was fixed from `timestamp` to `timestamp with local time zone`.
  Unfortunately, Hive doesn't allow change column types in external table definitions, so you must
  drop the external table and then recreate it.

## 2.0.3 - 2023-06-22

* GA release.

## 2.0.2-preview - 2023-06-08

* PR #85: Remove support for deprecated table properties bq.project and bq.dataset
* PR #83: Add BigQuery BigNumeric type into decimal support
* PR #82: Upgrade BigQuery-Connector-Common to 0.31.0. Add Impersonation support.
* PR #80: Fixed indirect write on not-null fields failure.

## 2.0.1-preview - 2023-05-17

* PR #69: Fixed dependency shading, reduced the distributable jar size

## 2.0.0-preview - 2023-04-12

* The first preview release.
