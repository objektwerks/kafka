package kafka

import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.Logger
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.io.Source

class KafkaTest extends FunSuite with BeforeAndAfterAll with Matchers {
  val logger = Logger.getLogger(classOf[KafkaTest])
  val producer = new KafkaProducer[String, String](loadProperties("/kafka.producer.properties"))
  val consumer = new KafkaConsumer[String, String](loadProperties("/kafka.consumer.properties"))
  val kafkaTopic = "kv"

  override protected def beforeAll(): Unit = {
    val zkClient = ZkUtils.createZkClient("localhost:2181", 10000, 10000)
    val zkUtils = ZkUtils(zkClient, isZkSecurityEnabled = false)
    val topicMetadata = AdminUtils.fetchTopicMetadataFromZk(kafkaTopic, zkUtils)
    logger.info(s"Kafka topic: ${topicMetadata.topic}")
    if (topicMetadata.topic != kafkaTopic) {
      AdminUtils.createTopic(zkUtils, kafkaTopic, 1, 1, loadProperties("/kafka.producer.properties"))
      logger.info(s"Kafka Topic ( $kafkaTopic ) created.")
    }
  }

  test("kafka") {
    produceMessages(3) shouldBe 3
    consumeMessages(3) min 3
  }

  def produceMessages(count: Int): Int = {
    val messages = new AtomicInteger()
    for (i <- 1 to count) {
      val key = i.toString
      val record = new ProducerRecord[String, String](kafkaTopic, 0, key, key)
      producer.send(record)
      logger.info(s"Producer -> key: $key value: ${record.value}")
      messages.incrementAndGet()
    }
    producer.close(3000L, TimeUnit.MILLISECONDS)
    messages.get
  }

  def consumeMessages(count: Int): Int = {
    consumer.subscribe(util.Arrays.asList(kafkaTopic))
    val messages = new AtomicInteger()
    while (messages.get < count) {
      val records = consumer.poll(1000L)
      val iterator = records.iterator()
      while (iterator.hasNext) {
        val record = iterator.next
        logger.info(s"Consumer -> key: ${record.key} value: ${record.value}")
        messages.incrementAndGet()
      }
    }
    messages.get
  }

  def loadProperties(file: String): Properties = {
    val properties = new Properties()
    properties.load(Source.fromInputStream(getClass.getResourceAsStream(file)).bufferedReader())
    properties
  }
}