# Changelog
All notable changes to ModelarDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- A storage layer that use Apache Parquet or Apache ORC as the on-disk format.

### Changed
- The Apache Spark-based engine now uses INT64 to represent timestamps when
  reading and writing Apache Parquet files for compatibility with the H2-based
  engine.

### Deprecated
### Removed
### Fixed
### Security

## [0.1.0] - 2021-07-07
### Added
- A query engine for single-node deployments based on the RDBMS H2.
- Support for executing queries from multiple users in parallel when using the
  socket and HTTP interface.
- All UDFs and UDAFs in the Segment View now support the # operator, in
  contrast to * it selects only the columns required for a UDAF or UDF.
- Support for creating queryable derived time series as a source time series and
  a function defined as `transform(value: Float, scalingFactor: Float): Float`.
- Support for ingesting time series from Apache Parquet and Apache ORC.

### Changed
- The configuration file now defaults to $HOME/.modelardb.conf.
- Improved names of parameters in the configuration file.
- The timezone in the configuration is set as the default so it is always used.
- Changed the column names in the views to match internal terminology.
- The UDF INTERVAL in the Segment View has been renamed to START_END.

### Removed
- SQLite is no longer included as it is replaced by H2.
- The _SS UDAFs have been removed as they are not needed for H2 and can be
  replaced with INLINE and a sub-query for Apache Spark.
- The Spark Cassandra Connector is no longer included in the uber Jar.

## [0.0.0] - 2020-12-10
### Added
- Research Prototype
