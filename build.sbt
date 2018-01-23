name := "MBI"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "com.github.samtools" % "htsjdk" % "2.14.0",
  "org.apache.spark" %% "spark-sql" % "2.2.1",
  "org.scalatest" %% "scalatest" % "3.0.4" % Test,
  "junit" % "junit" % "4.12" % Test,
)