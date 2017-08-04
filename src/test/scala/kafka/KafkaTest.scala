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
  val topic = "kv"

  test("kafka") {
    assertTopic()
    produceMessages(3)
    consumeMessages(3) min 3
  }

  def produceMessages(count: Int): Unit = {
    val producer = new KafkaProducer[String, String](loadProperties("/kafka.producer.properties"))
    for (i <- 1 to count) {
      val key = i.toString
      val value = key
      val record = new ProducerRecord[String, String](topic, key, value)
      val metadata = producer.send(record).get()
      logger.info(s"Producer -> topic: ${metadata.topic} partition: ${metadata.partition} offset: ${metadata.offset}")
      logger.info(s"Producer -> key: $key value: ${record.value}")
    }
    producer.flush()
    producer.close(1000L, TimeUnit.MILLISECONDS)
  }

  def consumeMessages(retries: Int): Int = {
    val consumer = new KafkaConsumer[String, String](loadProperties("/kafka.consumer.properties"))
    consumer.subscribe(java.util.Arrays.asList(topic))
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

  def loadProperties(file: String): Properties = {
    val properties = new Properties()
    properties.load(Source.fromInputStream(getClass.getResourceAsStream(file)).bufferedReader())
    properties
  }

  def assertTopic(): Unit = {
    val zkClient = ZkUtils.createZkClient("localhost:2181", 10000, 10000)
    val zkUtils = ZkUtils(zkClient, isZkSecurityEnabled = false)
    val metadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkUtils)
    logger.info(s"Kafka topic: ${metadata.topic}")
    zkClient.close()
  }
}