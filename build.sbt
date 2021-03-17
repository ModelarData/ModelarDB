name := "ModelarDB"
version := "1.0"
scalaVersion := "2.12.13"
scalacOptions ++= Seq("-opt:l:inline", "-opt-inline-from:<sources>", "-feature", "-deprecation", "-Xlint:_")

resolvers += "Repo at github.com/ankurdave/maven-repo" at "https://github.com/ankurdave/maven-repo/raw/master"
resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  "org.apache.derby" % "derby" % "10.15.2.0",
  "org.hsqldb" % "hsqldb" % "2.5.1",
  "com.h2database" % "h2" % "1.4.200",
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.0.0",
  "amplab" % "spark-indexedrdd" % "0.4.0",
  "org.apache.spark" %% "spark-core" % "3.1.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "3.1.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.1.1" % "provided",

  "org.scalatest" %% "scalatest" % "3.2.5" % Test,
  "org.scalacheck" %% "scalacheck" % "1.14.1" % Test,
  "org.scalamock" %% "scalamock" % "5.1.0" % Test
)

/* To avoid assembly conflict with Derby classes */
assemblyMergeStrategy in assembly := {
  case PathList("module-info.class") => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated

/* Otherwise Derby throws a java.security.AccessControlException in tests */
Test / testOptions += Tests.Setup(() => System.setSecurityManager(null))

/* Disable log buffering when running tests for nicer output */
logBuffered in Test := false

githubOwner := "modelardata"
githubRepository := "modelardb"
githubTokenSource := TokenSource.Environment("GITHUB_TOKEN")
