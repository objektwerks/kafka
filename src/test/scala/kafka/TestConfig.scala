package kafka

import java.util.Properties

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}

import scala.io.Source

object TestConfig {
  val kafkaConsumerKeyValueProperties = loadProperties("/kafka.consumer.key.value.properties")
  val kafkaConsumerWordCountProperties = loadProperties("/kafka.consumer.word.count.properties")
  val kafkaProducerProperties = loadProperties("/kafka.producer.properties")
  val kafkaStreamProperties = loadProperties("/kafka.stream.properties")

  val keyValueTopic = "keyvalue"
  val wordTopic = "word"
  val wordCountTopic = "wordcount"

  val gettysburgAddress = Source.fromInputStream(getClass.getResourceAsStream("/gettysburg.address.txt")).getLines.toSeq

  val adminClientProperties = new Properties()
  adminClientProperties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  def loadProperties(file: String): Properties = {
    val properties = new Properties()
    properties.load(Source.fromInputStream(getClass.getResourceAsStream(file)).bufferedReader())
    properties
  }

  def assertTopic(topic: String): Boolean = {
    import collection.JavaConverters._

    val adminClient: AdminClient = AdminClient.create(adminClientProperties)
    val newTopic = new NewTopic(topic, 1, 1.toShort)
    val createTopicResult = adminClient.createTopics(List(newTopic).asJavaCollection)
    createTopicResult.values().containsKey(topic)
  }
}