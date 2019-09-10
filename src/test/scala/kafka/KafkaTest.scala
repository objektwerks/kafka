package kafka

import java.time.Duration
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.KafkaException
import org.scalatest.{FunSuite, Matchers}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.io.Source

class KafkaTest extends FunSuite with Matchers {
  val logger = LoggerFactory.getLogger(getClass.getSimpleName)
  val kafkaConsumerProperties = loadProperties("/kafka-consumer.properties")
  val kafkaProducerProperties = loadProperties("/kafka-producer.properties")
  val kafkaConsumerTxProperties = loadProperties("/kafka-consumer-tx.properties")
  val kafkaProducerTxProperties = loadProperties("/kafka-producer-tx.properties")
  val adminClientProperties = new Properties()
  adminClientProperties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  test("at-least-once") {
    val topic = "keyvalue"
    createTopic(topic) shouldBe true

    produceMessages(topic, 3)
    val postProduceMessageCount = countMessages(topic, kafkaConsumerProperties)

    consumeMessages(topic, kafkaConsumerProperties)
    val postConsumeMessageCount = countMessages(topic, kafkaConsumerProperties)

    postProduceMessageCount should be >= 3
    postConsumeMessageCount shouldEqual 0
  }

  test("exactly-once") {
    val topic = "keyvalue-tx"
    createTopic(topic) shouldBe true

    produceTxMessages(topic, 3)
    val postProduceTxMessageCount = countMessages(topic, kafkaConsumerTxProperties)

    consumeMessages(topic, kafkaConsumerTxProperties)
    val postConsumeTxMessageCount = countMessages(topic, kafkaConsumerTxProperties)

    postProduceTxMessageCount should be >= 3
    postConsumeTxMessageCount shouldEqual 0
  }

  def createTopic(topic: String): Boolean = {
    val adminClient = AdminClient.create(adminClientProperties)
    val newTopic = new NewTopic(topic, 1, 1.toShort)
    val createTopicResult = adminClient.createTopics(List(newTopic).asJavaCollection)
    createTopicResult.values().containsKey(topic)
  }

  def produceMessage(i: Int, producer: KafkaProducer[String, String], topic: String): Unit = {
    val key = i.toString
    val value = key
    val record = new ProducerRecord[String, String](topic, key, value)
    val metadata = producer.send(record).get()
    logger.info(s"*** Producer -> topic: ${metadata.topic} partition: ${metadata.partition} offset: ${metadata.offset}")
    logger.info(s"*** Producer -> key: ${record.key} value: ${record.value}")
  }

  def produceMessages(topic: String, count: Int): Unit = {
    val producer = new KafkaProducer[String, String](kafkaProducerProperties)
    for (i <- 1 to count) produceMessage(i, producer, topic)
    producer.flush()
    producer.close()
  }

  def produceTxMessages(topic: String, count: Int): Unit = {
    val producer = new KafkaProducer[String, String](kafkaProducerTxProperties)
    producer.initTransactions()
    try {
      producer.beginTransaction()
      for (i <- 1 to count) produceMessage(i, producer, topic)
      producer.commitTransaction()
    } catch {
      case error: KafkaException =>
        logger.error(s"*** Tx Producer error: $error")
        producer.abortTransaction()
    } finally {
      producer.close()
    }
  }

  def consumeMessages(topic: String, properties: Properties): Unit = {
    val consumer = new KafkaConsumer[String, String](properties)
    consumer.subscribe(List(topic).asJava)
    for (i <- 1 to 2) {
      val records = consumer.poll(Duration.ofMillis(100L))
      logger.info(s"*** Consumer -> { ${records.count} } records polled on attempt { $i }.")
      records.iterator.asScala.foreach { record =>
        logger.info(s"*** Consumer -> topic: ${record.topic} partition: ${record.partition} offset: ${record.offset} key: ${record.key} value: ${record.value}")
      }
      if (records.count > 0) consumer.commitSync()
    }
    consumer.close()
  }

  def countMessages(topic: String, properties: Properties): Int = {
    val consumer = new KafkaConsumer[String, String](properties)
    consumer.subscribe(List(topic).asJava)
    val count = new AtomicInteger()
    for (_ <- 1 to 2) {
      val records = consumer.poll(Duration.ofMillis(100L))
      records.iterator.asScala.foreach { _ => count.incrementAndGet }
    }
    consumer.close()
    logger.info(s"+++ Consumer -> record count is ${count.get} for topic: $topic")
    count.get
  }

  def loadProperties(file: String): Properties = {
    val properties = new Properties()
    properties.load(Source.fromInputStream(getClass.getResourceAsStream(file)).bufferedReader())
    properties
  }
}