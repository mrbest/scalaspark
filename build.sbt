name := "SparkScalaDfHandling"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.3.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.3.0",
  "com.github.nscala-time" %% "nscala-time" % "2.20.0"
)