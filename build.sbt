name := "ModelarDB"
scalaVersion := "2.12.14"
scalacOptions ++= Seq("-opt:l:inline", "-opt-inline-from:<sources>", "-feature", "-deprecation", "-Xlint:_")

libraryDependencies ++= Seq(
  /* Code Generation */
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,

  /* Query Interface */
  "org.apache.arrow" % "flight-core" % "6.0.1",
  "org.apache.arrow" % "arrow-jdbc" % "6.0.1",

  /* Query Engine */
  "com.h2database" % "h2" % "1.4.200",
  "org.apache.spark" %% "spark-core" % "3.1.2" % "provided",
  "org.apache.spark" %% "spark-streaming" % "3.1.2" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.1.2" % "provided",

  /* Storage Layer */
  //H2 is a full RDBMS with both a query engine and a storage layer
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.0.1" % "provided", //Requires Spark
  "org.apache.hadoop" % "hadoop-client" % "3.2.0", //Same as Apache Spark
  "org.apache.parquet" % "parquet-hadoop" % "1.10.1", //Same as Apache Spark
  "org.apache.orc" % "orc-core" % "1.5.12", //Same as Apache Spark

  /* Testing */
  "org.scalatest" %% "scalatest" % "3.2.9" % Test,
  "org.scalacheck" %% "scalacheck" % "1.15.4" % Test,
  "org.scalamock" %% "scalamock" % "5.1.0" % Test
)

/* Apache Spark raises ExceptionInInitializerError on startup if an incorrect version is used*/
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.10.0"

/* Makes SBT include the dependencies marked as provided when run */
Compile / run := Defaults.runTask(
  Compile / fullClasspath,
  Compile / run / mainClass,
  Compile / run / runner).evaluated

/* Disables log buffering when running tests for nicer output */
Test / logBuffered := false

/* Use Ivy instead of Coursier due to Coursier GitHub Issue #2016 */
ThisBuild / useCoursier := false

/* Creates a code coverage report in HTML using Jacoco */
jacocoReportSettings := JacocoReportSettings(formats = Seq(JacocoReportFormats.ScalaHTML))

/* Github Package Repository */
val owner = "ModelarData"
val repo = "ModelarDB"
publishMavenStyle := true
publishTo := Some("GitHub Package Registry" at s"https://maven.pkg.github.com/$owner/$repo")

credentials +=
Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  "_", // The username is ignored when using a GITHUB_TOKEN is used for login
  sys.env.getOrElse("GITHUB_TOKEN", "") // getOrElse allows SBT to always run
)
