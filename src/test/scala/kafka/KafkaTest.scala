package kafka

import java.time.Duration

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.scalatest.{FunSuite, Matchers}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class KafkaTest extends FunSuite with Matchers {
  implicit val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
  val topic = "keyvalue"
  val topicTx = "keyvalue-tx"

  test("producer -> consumer") {
    import KafkaCommon._
    assertTopic(topic) shouldBe true

    produceMessages(topic, 3)
    val postProduceMessageCount = countMessages(topic)

    consumeMessages(topic)
    val postConsumeMessageCount = countMessages(topic)

    postProduceMessageCount should be >= 3
    postConsumeMessageCount shouldEqual 0
  }

  def produceMessages(topic: String, count: Int): Unit = {
    val producer = new KafkaProducer[String, String](KafkaCommon.kafkaProducerProperties)
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
    val consumer = new KafkaConsumer[String, String](KafkaCommon.kafkaConsumerProperties)
    consumer.subscribe(List(topic).asJava)
    for (i <- 1 to 2) {
      val records = consumer.poll(Duration.ofMillis(100L))
      logger.info(s"*** Consumer -> { ${records.count} } records polled on attempt { $i }.")
      records.iterator.asScala.foreach { record =>
        logger.info(s"*** Consumer -> topic: ${record.topic} partition: ${record.partition} offset: ${record.offset} key: ${record.key} value: ${record.value}")
      }
      if (records.count > 0) consumer.commitAsync()
    }
    consumer.close()
  }
}