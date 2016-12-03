package kafka

import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.scalatest.FunSuite

import scala.io.Source

class KafkaTest extends FunSuite {
  val producerProperties = loadProperties("/kafka.producer.properties")
  val consumerProperties = loadProperties("/kafka.consumer.properties")
  val kafkaTopic = "kv"

  test("kafka") {
    createKafkaTopic()
    assert(produceAndSendKafkaTopicMessages(3) == 3)
    assert(consumeKafkaTopicMessages(3) >= 3)
  }

  private def createKafkaTopic(): Unit = {
    val zkClient = ZkUtils.createZkClient("localhost:2181", 10000, 10000)
    val zkUtils = ZkUtils(zkClient, isZkSecurityEnabled = false)
    val topicMetadata = AdminUtils.fetchTopicMetadataFromZk(kafkaTopic, zkUtils)
    println(s"Kafka topic: ${topicMetadata.topic}")
    if (topicMetadata.topic != kafkaTopic) {
      AdminUtils.createTopic(zkUtils, kafkaTopic, 1, 1, producerProperties)
      println(s"Kafka Topic ( $kafkaTopic ) created.")
    }
  }

  private def produceAndSendKafkaTopicMessages(count: Int): Int = {
    val producer = new KafkaProducer[String, String](producerProperties)
    val messages = new AtomicInteger()
    for (i <- 1 to count) {
      val key = i.toString
      val record = new ProducerRecord[String, String](kafkaTopic, 0, key, key)
      producer.send(record)
      println(s"Produced -> key: $key value: ${record.value}")
      messages.incrementAndGet()
    }
    producer.close(3000L, TimeUnit.MILLISECONDS)
    messages.get
  }

  private def consumeKafkaTopicMessages(count: Int): Int = {
    val consumer = new KafkaConsumer[String, String](consumerProperties)
    consumer.subscribe(util.Arrays.asList(kafkaTopic))
    val messages = new AtomicInteger()
    while (messages.get < count) {
      val records = consumer.poll(1000L)
      val iterator = records.iterator()
      while (iterator.hasNext) {
        val record = iterator.next
        println(s"Consumed -> key: ${record.key} value: ${record.value}")
        messages.incrementAndGet()
      }
    }
    messages.get
  }

  private def loadProperties(file: String): Properties = {
    val properties = new Properties()
    properties.load(Source.fromInputStream(getClass.getResourceAsStream(file)).bufferedReader())
    properties
  }
}