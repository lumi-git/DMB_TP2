ThisBuild / version := "0.1.0-SNAPSHOT"

scalaVersion := "2.12.10" // Use a version compatible with Spark 3.1.1


lazy val root = (project in file("."))
  .settings(
    name := "TP2DMB"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.1",
  "org.apache.spark" %% "spark-sql" % "3.1.1",
"org.apache.spark" %% "spark-graphx" % "3.1.1"
)