name := "kafka"
organization := "objektwerks"
version := "0.1-SNAPSHOT"
scalaVersion := "2.12.8"
libraryDependencies ++= {
  val kafkaVersion = "2.1.0"
  Seq(
    "org.apache.kafka" %% "kafka" % kafkaVersion,
    "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion,
    "org.apache.kafka" % "kafka-streams-test-utils" % kafkaVersion % Test,
    "org.scalatest" %% "scalatest" % "3.0.5" % Test
  )
}
excludeDependencies += ExclusionRule("javax.ws.rs", "javax.ws.rs-api")
libraryDependencies += "javax.ws.rs" % "javax.ws.rs-api" % "2.1.1"