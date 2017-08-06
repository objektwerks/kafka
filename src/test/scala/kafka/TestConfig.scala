package kafka

import java.util.Properties

import kafka.admin.AdminUtils
import kafka.utils.ZkUtils

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

  def loadProperties(file: String): Properties = {
    val properties = new Properties()
    properties.load(Source.fromInputStream(getClass.getResourceAsStream(file)).bufferedReader())
    properties
  }

  def assertTopic(topic: String): String = {
    val zkClient = ZkUtils.createZkClient("localhost:2181", 10000, 10000)
    val zkUtils = ZkUtils(zkClient, isZkSecurityEnabled = false)
    val metadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkUtils)
    zkClient.close()
    metadata.topic
  }
}