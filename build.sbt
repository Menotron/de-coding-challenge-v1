name := "de-coding-challenge-v1"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.12" % "3.1.0",
  "org.apache.spark" % "spark-sql_2.12" % "3.1.0",
  "com.typesafe" % "config" % "1.2.0"
)
