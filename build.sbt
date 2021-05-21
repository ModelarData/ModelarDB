name := "ModelarDB"
version := "1.0"
scalaVersion := "2.12.13"
scalacOptions ++= Seq("-opt:l:inline", "-opt-inline-from:<sources>", "-feature", "-deprecation", "-Xlint:_")

val AkkaVersion = "2.6.13"
val SparkVersion = "3.1.1"

libraryDependencies ++= Seq(
  /* Code Generation */
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,

  /* Query Engine */
  "com.h2database" % "h2" % "1.4.200",
  "org.apache.spark" %% "spark-core" % "3.1.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "3.1.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.1.1" % "provided",

  /* Storage Layer */
  //H2 is a full RDBMS with both a query engine and a storage layer
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.0.1",
  "org.apache.hadoop" % "hadoop-client" % "3.2.0", //Same as Apache Spark
  "org.apache.parquet" % "parquet-hadoop" % "1.10.1", //Same as Apache Spark
  "org.apache.orc" % "orc-core" % "1.5.12", //Same as Apache Spark
  "org.xerial" % "sqlite-jdbc" % "3.34.0",

  /* Logging and Config */
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.3",
  "com.github.pureconfig" %% "pureconfig" % "0.15.0",

  /* Akka */
  "com.lightbend.akka" %% "akka-stream-alpakka-mqtt-streaming" % "2.0.2",
  "com.typesafe.akka" %% "akka-stream-typed" % AkkaVersion,
  "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,

  /* Arrow */
  "org.apache.arrow" % "flight-grpc" % "3.0.0",
  "org.apache.arrow" % "arrow-jdbc" % "3.0.0",

  /* Testing */
  "org.scalatest" %% "scalatest" % "3.2.5" % Test,
  "org.scalacheck" %% "scalacheck" % "1.14.1" % Test,
  "org.scalamock" %% "scalamock" % "5.1.0" % Test
)

/* Makes SBT include the dependencies marked as provided when run */
Compile / run := Defaults.runTask(
  Compile / fullClasspath,
  Compile / run / mainClass,
  Compile / run / runner).evaluated

/* Disables log buffering when running tests for nicer output */
Test / logBuffered := false

/* Otherwise Derby throws a java.security.AccessControlException in tests */
Test / testOptions += Tests.Setup(() => System.setSecurityManager(null))

/* To avoid assembly conflict with Derby and Arrow classes */
assemblyMergeStrategy in assembly := {
  case PathList("module-info.class") => MergeStrategy.first
  case "META-INF/io.netty.versions.properties" => MergeStrategy.first
  case "google/protobuf/compiler/plugin.proto" => MergeStrategy.first
  case "google/protobuf/compiler/descriptor.proto" => MergeStrategy.first
  case "google/protobuf/descriptor.proto" => MergeStrategy.first
  case "git.properties" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

mainClass in assembly := Some("dk.aau.modelardb.Main")

/* Creates a code coverage report in HTML using Jacoco */
jacocoReportSettings := JacocoReportSettings(formats = Seq(JacocoReportFormats.ScalaHTML))

/* Github Package Repository */
val owner = "modelardata"
val repo = "modelardb"
publishMavenStyle := true
publishTo := Some("GitHub Package Registry" at s"https://maven.pkg.github.com/$owner/$repo")

credentials +=
Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  "_", // The username is ignored when a GITHUB_TOKEN is used for login
  sys.env.getOrElse("GITHUB_TOKEN", "") // getOrElse allows SBT to always run
)

