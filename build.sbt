name := "flight-data"

organization := "com.quantexa"
scalaVersion := "2.12.10"
version := "0.1"
val versions = Map(
  'spark -> "2.4.4",
  'hadoop -> "2.6.5",
  'scalatest -> "3.0.8",
  'spark_fast_tests -> "0.20.0-s_2.12",
  'scala_nameof -> "1.0.3",
)

resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % versions('spark) % Provided,
  "org.apache.spark" %% "spark-sql" % versions('spark) % Provided,
  "org.apache.hadoop" % "hadoop-common" % versions('hadoop) % Provided,
  "org.scalatest" %% "scalatest" % versions('scalatest) % Test,
  "MrPowers" % "spark-fast-tests" % versions('spark_fast_tests) % Test,
  // Compile-time only, so Provided
  "com.github.dwickern" %% "scala-nameof" % versions('scala_nameof) % Provided,
)

dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7.1",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.1",
)

// Set assembly name
assemblyJarName in assembly := s"${name.value}_${scalaBinaryVersion.value}-${version.value}.jar"

Compile / mainClass := Some("com.quantexa.flightdata.StatsApp")
mainClass in assembly := Some("com.quantexa.flightdata.StatsApp")
