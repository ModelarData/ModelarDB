name := "ModelarDB"
version := "1.0"
scalaVersion := "2.12.13"
scalacOptions ++= Seq("-opt:l:inline", "-opt-inline-from:<sources>", "-feature", "-deprecation", "-Xlint:_")

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  "com.h2database" % "h2" % "1.4.200",
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.0.0",
  "org.apache.spark" %% "spark-core" % "3.1.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "3.1.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.1.1" % "provided",

  "org.scalatest" %% "scalatest" % "3.2.5" % Test,
  "org.scalacheck" %% "scalacheck" % "1.14.1" % Test,
  "org.scalamock" %% "scalamock" % "5.1.0" % Test
)

/* Makes SBT include the dependencies marked as provided when run */
(run in Compile) :=
  Defaults.runTask(
    fullClasspath in Compile,
    mainClass in (Compile, run),
    runner in (Compile, run)
  ).evaluated

/* Disables log buffering when running tests for nicer output */
logBuffered in Test := false

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
    "_", // The username is ignored when using a GITHUB_TOKEN is used for login
    sys.env.getOrElse("GITHUB_TOKEN", "") // getOrElse allows SBT to always run
  )