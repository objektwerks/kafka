package kafka

import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.Logger
import org.scalatest.{FunSuite, Matchers}

import scala.io.Source

class KafkaTest extends FunSuite with Matchers {
  val logger = Logger.getLogger(classOf[KafkaTest])
  val kafkaTopic = "objektwerks"

  test("kafka") {
    connectToZookeeper()
    produceMessages(3) shouldBe 3
    consumeMessages(3) min 3
  }

  def produceMessages(count: Int): Int = {
    val producer = new KafkaProducer[String, String](loadProperties("/kafka.producer.properties"))
    val messages = new AtomicInteger()
    for (i <- 1 to count) {
      val key = i.toString
      val record = new ProducerRecord[String, String](kafkaTopic, 0, key, key)
      val future = producer.send(record)
      val metadata = future.get(1000L, TimeUnit.MILLISECONDS)
      logger.info(s"Producer -> topic: ${metadata.topic} partition: ${metadata.partition} offset: ${metadata.offset}")
      logger.info(s"Producer -> key: $key value: ${record.value}")
      messages.incrementAndGet()
    }
    producer.close(1000L, TimeUnit.MILLISECONDS)
    messages.get
  }

  def consumeMessages(count: Int): Int = {
    val consumer = new KafkaConsumer[String, String](loadProperties("/kafka.consumer.properties"))
    consumer.subscribe(java.util.Arrays.asList(kafkaTopic))
    val messages = new AtomicInteger()
    logger.info(s"Consumer messages to poll count: $count")
    logger.info(s"Consumer messages polled initial count: ${messages.get()}")
    while (messages.get < count) {
      logger.info(s"Consumer messages current polled count: ${messages.get()}")
      val records = consumer.poll(100L)
      logger.info(s"Consumer poll returned with ${records.count()} records.")
      val iterator = records.iterator()
      while (iterator.hasNext) {
        val record = iterator.next
        logger.info(s"Consumer -> key: ${record.key} value: ${record.value}")
        messages.incrementAndGet()
      }
    }
    consumer.close(1000L, TimeUnit.MILLISECONDS)
    messages.get
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