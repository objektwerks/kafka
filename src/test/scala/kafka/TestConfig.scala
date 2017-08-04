package kafka

import java.util.Properties

import scala.io.Source

object TestConfig {
  val kafkaConsumerProperties = loadProperties("/kafka.consumer.properties")
  val kafkaProducerProperties = loadProperties("/kafka.producer.properties")

  def loadProperties(file: String): Properties = {
    val properties = new Properties()
    properties.load(Source.fromInputStream(getClass.getResourceAsStream(file)).bufferedReader())
    properties
  }
}