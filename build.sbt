name := "kafka"
organization := "objektwerks"
version := "0.1-SNAPSHOT"
scalaVersion := "2.13.18"
libraryDependencies ++= {
  Seq(
    "org.apache.kafka" %% "kafka" % "2.8.2", // Don't upgrade to version 3+, major changes required!
    "ch.qos.logback" % "logback-classic" % "1.5.21",
    "org.scalatest" %% "scalatest" % "3.2.19" % Test
  )
}
