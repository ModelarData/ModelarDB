name := "ModelarDB"
scalaVersion := "2.12.15"
scalacOptions ++= Seq("-opt:l:inline", "-opt-inline-from:<sources>", "-feature", "-deprecation", "-Xlint:_")

libraryDependencies ++= Seq(
  /* Code Generation */
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,

  /* Query Interface */
  "org.apache.arrow" % "flight-core" % "6.0.1",
  "org.apache.arrow" % "arrow-jdbc" % "6.0.1",

  /* Query Engine */
  "com.h2database" % "h2" % "1.4.200",
  "org.apache.spark" %% "spark-core" % "3.2.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "3.2.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.2.1" % "provided",

  /* Storage Layer */
  //H2 is a full RDBMS with both a query engine and a storage layer
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.1.0" % "provided", //Requires Spark
  "org.apache.hadoop" % "hadoop-client" % "3.2.0", //Same as Apache Spark 3.1.2 due to javax.xml.bind conflicts
  "org.apache.parquet" % "parquet-hadoop" % "1.12.2", //Same as Apache Spark
  "org.apache.orc" % "orc-core" % "1.6.13", //Same as Apache Spark

  /* Testing */
  "org.scalatest" %% "scalatest" % "3.2.9" % Test,
  "org.scalacheck" %% "scalacheck" % "1.15.4" % Test,
  "org.scalamock" %% "scalamock" % "5.1.0" % Test
)

/* Make SBT include the dependencies marked as provided when executing sbt run */
Compile / run := Defaults.runTask(
  Compile / fullClasspath,
  Compile / run / mainClass,
  Compile / run / runner).evaluated

/* Prevent Apache Spark from overwriting dependencies with older incompatible versions */
assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("com.google.**" -> "com.google.shaded.@1").inAll,
)

/* Concat and discard duplicate metadata in the dependencies when creating an assembly */
assembly / assemblyMergeStrategy := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.concat
  case "git.properties" => MergeStrategy.discard
  case "module-info.class" => MergeStrategy.discard
  case x => (assembly/assemblyMergeStrategy).value(x)
}

/* Use Ivy instead of Coursier due to Coursier GitHub Issue #2016 */
ThisBuild / useCoursier := false

/* Show the duration of tests and complete stack traces with color */
Test / testOptions += Tests.Argument("-oDF")

/* Execute tests sequentially to reduce the amount of memory requred */
Test / parallelExecution := false

/* Create a code coverage report in HTML using Jacoco */
jacocoReportSettings := JacocoReportSettings(formats = Seq(JacocoReportFormats.ScalaHTML))

/* GitHub Package Repository */
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
