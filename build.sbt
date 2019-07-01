name := "kafka"
organization := "objektwerks"
version := "0.1-SNAPSHOT"
scalaVersion := "2.12.8"
libraryDependencies ++= {
  Seq(
    "org.apache.kafka" %% "kafka" % "2.3.0",
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "org.scalatest" %% "scalatest" % "3.0.5" % Test
  )
}