name := "kafka"
organization := "objektwerks"
version := "0.1-SNAPSHOT"
scalaVersion := "2.13.6"
libraryDependencies ++= {
  Seq(
    "org.apache.kafka" %% "kafka" % "2.7.0",
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "org.scalatest" %% "scalatest" % "3.2.9" % Test
  )
}
