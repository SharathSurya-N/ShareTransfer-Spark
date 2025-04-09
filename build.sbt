ThisBuild / scalaVersion := "2.12.20"

lazy val root = (project in file("."))
  .settings(
    name := "SparkCassandraProject",
    version := "0.1",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.2.1",
      "org.apache.spark" %% "spark-sql" % "3.2.1",      
    )
  )
libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.12" % "3.2.0"
libraryDependencies += "joda-time" % "joda-time" % "2.10.14"
