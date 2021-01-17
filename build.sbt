name := "ModelarDB"
version := "1.0"
scalaVersion := "2.12.13"
scalacOptions ++= Seq("-opt:l:inline", "-opt-inline-from:<sources>", "-feature", "-deprecation", "-Xlint:_")

resolvers += "Repo at github.com/ankurdave/maven-repo" at "https://github.com/ankurdave/maven-repo/raw/master"
resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  "org.xerial" % "sqlite-jdbc" % "3.34.0",
  "org.apache.derby" % "derby" % "10.15.2.0",
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.0.0",
  "amplab" % "spark-indexedrdd" % "0.4.0",
  "org.apache.spark" %% "spark-core" % "3.0.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "3.0.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.0.1" % "provided")

run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated
