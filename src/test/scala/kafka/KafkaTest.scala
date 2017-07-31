package kafka

import java.util.Properties
import java.util.concurrent.TimeUnit

import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.Logger
import org.scalatest.{FunSuite, Matchers}

import scala.io.Source

class KafkaTest extends FunSuite with Matchers {
  val logger = Logger.getLogger(classOf[KafkaTest])
  val kafkaTopic = "kv"

  test("kafka") {
    connectToZookeeper()
    produceMessages(3)
    consumeMessages() min 3
  }

  def produceMessages(count: Int): Unit = {
    val producer = new KafkaProducer[String, String](loadProperties("/kafka.producer.properties"))
    for (i <- 1 to count) {
      val key = i.toString
      val record = new ProducerRecord[String, String](kafkaTopic, 0, key, key)
      val future = producer.send(record)
      val metadata = future.get(1000L, TimeUnit.MILLISECONDS)
      logger.info(s"Producer -> topic: ${metadata.topic} partition: ${metadata.partition} offset: ${metadata.offset}")
      logger.info(s"Producer -> key: $key value: ${record.value}")
    }
    producer.close(1000L, TimeUnit.MILLISECONDS)
  }

  def consumeMessages(): Int = {
    val consumer = new KafkaConsumer[String, String](loadProperties("/kafka.consumer.properties"))
    consumer.subscribe(java.util.Arrays.asList(kafkaTopic))
    logger.info(s"Consumer -> polling...")
    val records = consumer.poll(1000L)
    val count = records.count()
    logger.info(s"Consumer -> $count records polled.")
    val iterator = records.iterator()
    while (iterator.hasNext) {
      val record = iterator.next
      logger.info(s"Consumer -> key: ${record.key} value: ${record.value}")
    }
    consumer.close(1000L, TimeUnit.MILLISECONDS)
    count
  }

  def loadProperties(file: String): Properties = {
    val properties = new Properties()
    properties.load(Source.fromInputStream(getClass.getResourceAsStream(file)).bufferedReader())
    properties
  }

  def connectToZookeeper(): Unit = {
    val zkClient = ZkUtils.createZkClient("localhost:2181", 10000, 10000)
    val zkUtils = ZkUtils(zkClient, isZkSecurityEnabled = false)
    val metadata = AdminUtils.fetchTopicMetadataFromZk(kafkaTopic, zkUtils)
    logger.info(s"Kafka topic: ${metadata.topic}")
  }
}