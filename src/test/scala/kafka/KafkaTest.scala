package kafka

import java.time.Duration

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.KafkaException
import org.scalatest.{FunSuite, Matchers}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class KafkaTest extends FunSuite with Matchers {
  import KafkaCommon._

  implicit val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  test("producer -> consumer") {
    val topic = "keyvalue"
    assertTopic(topic) shouldBe true

    produceMessages(topic, 3)
    val postProduceMessageCount = countMessages(topic)

    consumeMessages(topic)
    val postConsumeMessageCount = countMessages(topic)

    postProduceMessageCount should be >= 3
    postConsumeMessageCount shouldEqual 0
  }

  test("tx-producer -> tx-consumer") {
    val topic = "keyvalue-tx"
    assertTopic(topic) shouldBe true

    produceTxMessages(topic, 3)
    val postProduceTxMessageCount = countMessages(topic)

    consumeTxMessages(topic)
    val postConsumeTxMessageCount = countMessages(topic)

    postProduceTxMessageCount should be >= 3
    postConsumeTxMessageCount shouldEqual 0
  }

  def produceMessages(topic: String, count: Int): Unit = {
    val producer = new KafkaProducer[String, String](kafkaProducerProperties)
    for (i <- 1 to count) {
      val key = i.toString
      val value = key
      val record = new ProducerRecord[String, String](topic, key, value)
      val metadata = producer.send(record).get()
      logger.info(s"*** Producer -> topic: ${metadata.topic} partition: ${metadata.partition} offset: ${metadata.offset}")
      logger.info(s"*** Producer -> key: ${record.key} value: ${record.value}")
    }
    producer.flush()
    producer.close()
  }

  def consumeMessages(topic: String): Unit = {
    val consumer = new KafkaConsumer[String, String](kafkaConsumerProperties)
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

  def produceTxMessages(topic: String, count: Int): Unit = {
    val producer = new KafkaProducer[String, String](kafkaProducerTxProperties)
    producer.initTransactions()
    try {
      producer.beginTransaction()
      for (i <- 1 to count) {
        val key = i.toString
        val value = key
        val record = new ProducerRecord[String, String](topic, key, value)
        val metadata = producer.send(record).get()
        logger.info(s"*** Tx Producer -> topic: ${metadata.topic} partition: ${metadata.partition} offset: ${metadata.offset}")
        logger.info(s"*** Tx Producer -> key: ${record.key} value: ${record.value}")
      }
      producer.commitTransaction()
    } catch {
      case error:KafkaException =>
        logger.error(s"*** Tx Producer error: $error")
        producer.abortTransaction()
    } finally {
      producer.flush()
      producer.close()
    }
  }

  def consumeTxMessages(topic: String): Unit = {
    val consumer = new KafkaConsumer[String, String](kafkaConsumerTxProperties)
    consumer.subscribe(List(topic).asJava)
    for (i <- 1 to 2) {
      val records = consumer.poll(Duration.ofMillis(100L))
      logger.info(s"*** Tx Consumer -> { ${records.count} } records polled on attempt { $i }.")
      records.iterator.asScala.foreach { record =>
        logger.info(s"*** Tx Consumer -> topic: ${record.topic} partition: ${record.partition} offset: ${record.offset} key: ${record.key} value: ${record.value}")
      }
      if (records.count > 0) consumer.commitAsync()
    }
    consumer.close()
  }
}