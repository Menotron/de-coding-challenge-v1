name := "de-coding-challenge-v1"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-sql_2.12" % "3.1.0" withSources(),
  "org.apache.spark" % "spark-core_2.12" % "3.1.0" withSources(),
  "com.typesafe" % "config" % "1.2.0"
)

assemblyJarName in assembly := "de-coding-challenge-v1.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case x => MergeStrategy.first
}