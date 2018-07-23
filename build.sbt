name := "kafka"
organization := "objektwerks"
version := "0.1-SNAPSHOT"
scalaVersion := "2.12.6"
libraryDependencies ++= {
  val kafkaVersion = "1.1.1"
  Seq(
    "org.apache.kafka" % "kafka_2.12" % kafkaVersion,
    "org.apache.kafka" % "kafka-streams" % kafkaVersion,
    "org.scalatest" % "scalatest_2.12" % "3.0.5" % "test"
  )
}
scalacOptions ++= Seq(
  "-language:postfixOps",
  "-language:reflectiveCalls",
  "-language:implicitConversions",
  "-language:higherKinds",
  "-feature",
  "-Ywarn-unused-import",
  "-Ywarn-unused",
  "-Ywarn-dead-code",
  "-unchecked",
  "-deprecation",
  "-Xfatal-warnings",
  "-Xlint:missing-interpolator",
  "-Xlint"
)
fork in test := true
javaOptions += "-server -Xss1m -Xmx2g"
