name := "flight-data"

organization := "com.quantexa"
scalaVersion := "2.12.10"
version := "0.1"
val versions = Map(
  'spark -> "2.4.4",
  'hadoop -> "3.1.0"
)

//resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % versions('spark) % Provided,
  "org.apache.spark" %% "spark-sql" % versions('spark) % Provided,
  "org.apache.hadoop" % "hadoop-common" % versions('hadoop) % Provided
)

// Set assembly name
//assemblyJarName in assembly := s"${name.value}_${scalaBinaryVersion.value}-${version.value}.jar"
//
//Compile / mainClass := Some("com.quantexa.flightdata.StatsApp")
//mainClass in assembly := Some("com.quantexa.flightdata.StatsApp")
