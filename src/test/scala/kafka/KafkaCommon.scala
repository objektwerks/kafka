package kafka

import java.util.Properties

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}

import scala.collection.JavaConverters._
import scala.io.Source

object KafkaCommon {
  val kafkaConsumerProperties = loadProperties("/kafka-consumer.properties")
  val kafkaProducerProperties = loadProperties("/kafka-producer.properties")
  val keyValueTopic = "keyvalue"

  val adminClientProperties = new Properties()
  adminClientProperties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  def loadProperties(file: String): Properties = {
    val properties = new Properties()
    properties.load(Source.fromInputStream(getClass.getResourceAsStream(file)).bufferedReader())
    properties
  }

  def assertTopic(topic: String): Boolean = {
    val adminClient: AdminClient = AdminClient.create(adminClientProperties)
    val newTopic = new NewTopic(topic, 1, 1.toShort)
    val createTopicResult = adminClient.createTopics(List(newTopic).asJavaCollection)
    createTopicResult.values().containsKey(topic)
  }
}