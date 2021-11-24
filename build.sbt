import com.typesafe.sbt.packager.Keys.scriptClasspath
import com.typesafe.sbt.packager.MappingsHelper.{fromClasspath, relative}

lazy val root = (project in file("."))
  .enablePlugins(JavaAppPackaging)
  .settings(
    name := "ModelarDB",
    version := "1.0"
  )
scalaVersion := "2.12.14"
scalacOptions ++= Seq("-opt:l:inline", "-opt-inline-from:<sources>", "-feature", "-deprecation", "-Xlint:_")

val AkkaVersion = "2.6.13"
val SparkVersion = "3.1.2"
val ArrowVersion = "5.0.0"

libraryDependencies ++= Seq(
  /* Code Generation */
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,

  /* Query Engine */
  "com.h2database" % "h2" % "1.4.200",
  "org.apache.spark" %% "spark-core" % SparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % SparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % SparkVersion % "provided",

  /* Storage Layer */
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.1.0" % "provided", //Requires Spark
  "org.apache.hadoop" % "hadoop-client" % "3.2.0" //Same as Apache Spark
    exclude("com.google.protobuf", "protobuf-java"),
  "org.apache.parquet" % "parquet-hadoop" % "1.10.1", //Same as Apache Spark
  "org.apache.orc" % "orc-core" % "1.5.12" //Same as Apache Spark
    exclude("com.google.protobuf", "protobuf-java"),

  /* Logging and Config */
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.3",
  "com.github.pureconfig" %% "pureconfig" % "0.15.0",

  /* Akka */
  "com.lightbend.akka" %% "akka-stream-alpakka-mqtt-streaming" % "2.0.2",
  "com.typesafe.akka" %% "akka-stream-typed" % AkkaVersion,
  "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,

  /* Arrow */
  "org.apache.arrow" % "flight-grpc" % ArrowVersion,
  "org.apache.arrow" % "arrow-jdbc" % ArrowVersion,

  "com.beachape" %% "enumeratum" % "1.7.0",
  "joda-time" % "joda-time" % "2.10.13",

  /* Testing */
  "org.scalatest" %% "scalatest" % "3.2.9" % Test,
  "org.scalacheck" %% "scalacheck" % "1.15.4" % Test,
  "org.scalamock" %% "scalamock" % "5.1.0" % Test
)

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.10.5.1"
dependencyOverrides += "com.google.inject" % "guice" % "4.2.3"
dependencyOverrides += "com.google.guava" % "guava" % "28.2-jre"
dependencyOverrides += "org.apache.commons" % "commons-lang3" % "3.8" // need >= 3.8 when using Java 11: https://issues.apache.org/jira/browse/LANG-1384
dependencyOverrides += "com.thoughtworks.paranamer" % "paranamer" % "2.8" // need >= 2.8 when using Java >= 8: https://stackoverflow.com/questions/53787624

/* Makes SBT include the dependencies marked as provided when run */
Compile / run := Defaults.runTask(
  Compile / fullClasspath,
  Compile / run / mainClass,
  Compile / run / runner).evaluated

/* Make SBT fork for all tasks so the JDBC is always available */
fork := true

/* Disables log buffering when running tests for nicer output */
Test / logBuffered := false

/* Otherwise Derby throws a java.security.AccessControlException in tests */
Test / testOptions += Tests.Setup(() => System.setSecurityManager(null))

assembly / assemblyJarName := "ModelarDB.jar"
assembly / mainClass := Some("dk.aau.modelardb.Main")

Compile / discoveredMainClasses := Seq()
Compile / mainClass := Some("dk.aau.modelardb.Main")

/* To avoid assembly conflict with Derby and Arrow classes */
assembly / assemblyMergeStrategy := {
      case "META-INF/io.netty.versions.properties" => MergeStrategy.concat
      case PathList("META-INF", "versions", "9", "module-info.class") => MergeStrategy.discard
      case PathList("module-info.class") => MergeStrategy.discard
      case "git.properties" => MergeStrategy.discard
      case "google/protobuf/compiler/plugin.proto" => MergeStrategy.discard
      case "google/protobuf/descriptor.proto" => MergeStrategy.discard
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
}

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
  "_", // The username is ignored when a GITHUB_TOKEN is used for login
  sys.env.getOrElse("GITHUB_TOKEN", "") // getOrElse allows SBT to always run
)

Universal / mappings ++= Map(
  file("conf/spark-test.conf") -> "modelardb.conf",
  file("README.md") -> "README.md",
  file("LICENSE") -> "LICENSE"
).toSeq

/* Full dist contains all dependencies (also ones marked 'provided')
 To build the full dist run this on the command-line:
 sbt -Ddist=full clean stage
 */
lazy val distType = sys.props.getOrElse("dist", "slim")

Universal / mappings ++= {
  distType match {
    case "slim" => Seq.empty
    case "full" =>
      val compileDep = (root / Compile / managedClasspath).value.toSet
      val runtimeDep = (root / Runtime / managedClasspath).value.toSet
      val provided = compileDep -- runtimeDep
      fromClasspath(provided.toSeq, "lib", _ => true)
  }
}

scriptClasspath ++= {
  distType match {
    case "slim" => Seq.empty
    case "full" =>
      val compileDep = (root / Compile / managedClasspath).value.toSet
      val runtimeDep = (root / Runtime / managedClasspath).value.toSet
      val provided = compileDep -- runtimeDep
      relative(
        provided.map(_.data).toSeq,
        Seq((Universal / stagingDirectory).value)
      ).map(_._2) // path is the 2 element of the tuple
  }
}