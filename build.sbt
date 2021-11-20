name := "kafka"
organization := "objektwerks"
version := "0.1-SNAPSHOT"
scalaVersion := "2.13.7"
libraryDependencies ++= {
  Seq(
    "org.apache.kafka" %% "kafka" % "2.8.1",
    "ch.qos.logback" % "logback-classic" % "1.2.7",
    "org.scalatest" %% "scalatest" % "3.2.10" % Test
  )
}
