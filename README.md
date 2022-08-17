<p align="center"><b><a href=https://github.com/modelardata/modelardb>The
JVM-based version of ModelarDB in this repository</a> is no longer being
actively developed as it has been deprecated in favor of a significantly more
efficient and user-friendly <a
href=https://github.com/modelardata/modelardb-rs>Rust-based
implementation.</a></b></p>

# ModelarDB
ModelarDB is a modular model-based time series management system that
interfaces a query engine and a data store with ModelarDB Core. This Core is a
self-contained, adaptive, and highly extensible Java library for automatic
online compression and efficient aggregation of time series. ModelarDB is
designed for Unix-like operating systems and is tested on Linux.

ModelarDB intentionally does not gather usage data. So, all users are highly
encouraged to post comments, suggestions, and bugs as GitHub issues, especially
if a limitation of ModelarDB prevents it from being used in a particular
domain.

## Installation
1. Install a Java Development Kit\*
2. Install the Scala Build Tool (sbt).
3. Clone the ModelarDB Git [repository](https://github.com/ModelarData/ModelarDB).
4. Start `sbt` in the root of the repository and run one of the following commands:

- `compile` compiles ModelarDB to Java class files.
- `package` compiles ModelarDB to a Jar file.
- `assembly` compiles ModelarDB to an uber Jar file.\*\*
- `run` executes ModelarDB's main method.\*\*\*
- `test` executes ModelarDB's unit and integration tests.\*\*\*\*

\* OpenJDK 11 and Oracle's Java SE Development Kit 11 have been tested.

\*\* To execute ModelarDB on an existing Apache Spark cluster, an uber Jar must
be created to ensure the necessary dependencies are included in a single Jar
file.

\*\*\* If `sbt run` is executed directly from the command-line, then the run
command and the arguments must be surrounded by quotes to pass the arguments to
ModelarDB: `sbt 'run arguments'`

\*\*\*\* The data for the integration tests are read from Apache ORC
files. The folder containing these files is retrieved from the
environment variable `MODELARDB_TEST_DATA_ORC`. The integration tests
may require additional memory which can be set through `SBT_OPTS`,
e.g., `SBT_OPTS="-Xmx8G" sbt test`.

## Configuration
ModelarDB requires that a configuration file is available at
`$HOME/.modelardb.conf` or is passed as the first command-line argument. This
file must specify the query processing engine and data store to use. An example
configuration is included as part of this repository.

## Documentation
For more information about using ModelarDB see the [user manual](https://github.com/ModelarData/ModelarDB/blob/master/docs/index.md).

## Development
The repository is organized using two sets of branches:
- A `master` branch with the latest stable features and fixes.
- Multiple unstable feature branches with features currently in development.
  These are named `dev/name-of-the-feature`.

All changes are tracked in [CHANGELOG.md](CHANGELOG.md) and its unreleased
section should be updated by commits that are added to the master branch.

## Contributions
Contributions to all aspects of ModelarDB are highly appreciated and do not
need to be in the form of code. For example, contributions can be:

- Helping other users.
- Writing documentation.
- Testing features and reporting bugs.
- Writing unit tests and integration tests.
- Fixing bugs in existing functionality.
- Refactoring existing functionality.
- Implementing new functionality.

Any questions or discussions regarding a possible contribution should be posted
in the appropriate GitHub issue if one exists, e.g., the bug report if it is a
bugfix, and as a new GitHub issue otherwise.

## License
ModelarDB is licensed under version 2.0 of the Apache License and a copy of the
license is bundled with the program.
