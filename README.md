# ModelarDB
ModelarDB is a modular model-based time series management system that interfaces
a query engine and a storage system with ModelarDB Core. This Core is a
self-contained, adaptive, and highly extensible Java library for automatic online
compression of time series. ModelarDB is designed for Unix-like operating
systems and tested on Linux and macOS.

ModelarDB intentionally does not gather usage data. So, all users are highly
encouraged to post comments, suggestions, and bugs as GitHub issues, especially
if a limitation of ModelarDB prevents it from being used in a particular domain.

## Installation
1. Install the Java Development Kit\*
2. Install the Scala Build Tool (sbt).
3. Clone the ModelarDB Git repository https://github.com/ModelarData/ModelarDB
4. Start `sbt` in the root of the repository and run one of the following commands:

- `compile` compiles ModelarDB to Java class files.
- `package` compiles ModelarDB to a Jar file.
- `assembly` compiles ModelarDB to an uber Jar file.\*\*
- `run` executes ModelarDB's main method.\*\*\*

\* OpenJDK 11 and Oracle's Java SE Development Kit 11 have been tested.

\* To execute ModelarDB on an existing Apache Spark cluster, an uber Jar must be
created to ensure the necessary dependencies are all included in a single Jar
file.

\*\* If `sbt run` is executed directly from the command-line, then the run
command and the arguments must be surrounded by quotes to pass the arguments to
ModelarDB: `sbt 'run arguments'`

## Configuration
ModelarDB requires that a configuration file is available at
*$HOME/.modelardb.conf* or as the first command-line argument. This file must
specify the query processing engine and storage system to use. An example
configuration is included as part of this repository.

## Development
The repository is organized using three set branches:
- A `master` branch with the latest stable release.
- A `develop` branch with the latest features and fixes.
- Multiple unstable feature branches with features currently in development.
These are named `dev/name-of-the-feature`.

The `develop` branch should always compile without any errors but it is only
intended to be used for testing new features and bugfixes. A Docker image with a
recent known-good build from the `develop` branch and the [REDD data
set](http://redd.csail.mit.edu/) is
[available](https://github.com/orgs/ModelarData/packages?repo_name=ModelarDB)
for testing.

All changed are tracked in [CHANGELOG.md](CHANGELOG.md) and its unreleased
section should be updated when commits are added to the develop branch.

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
