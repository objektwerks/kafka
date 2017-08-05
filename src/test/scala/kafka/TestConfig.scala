package kafka

import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.log4j.Logger

import scala.collection.JavaConverters._
import scala.io.Source

object TestConfig {
  val logger = Logger.getLogger("KafkaTest")

  val kafkaConsumerProperties = loadProperties("/kafka.consumer.properties")
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

  def consumeMessages(topic: String, retries: Int): Int = {
    val consumer = new KafkaConsumer[String, String](TestConfig.kafkaConsumerProperties)
    consumer.subscribe(List(topic).asJava)
    val count = new AtomicInteger()
    for (i <- 1 to retries) {
      logger.info(s"Consumer -> polling attempt $i ...")
      val records = consumer.poll(100L)
      logger.info(s"Consumer -> ${records.count} records polled.")
      val iterator = records.iterator()
      while (iterator.hasNext) {
        val record = iterator.next
        logger.info(s"Consumer -> topic: ${record.topic} partition: ${record.partition} offset: ${record.offset}")
        logger.info(s"Consumer -> key: ${record.key} value: ${record.value}")
        count.incrementAndGet()
      }
    }
    consumer.close(1000L, TimeUnit.MILLISECONDS)
    count.get
  }
}