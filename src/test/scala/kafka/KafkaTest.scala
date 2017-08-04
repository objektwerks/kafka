package kafka

import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.Logger
import org.scalatest.{FunSuite, Matchers}

class KafkaTest extends FunSuite with Matchers {
  val logger = Logger.getLogger(classOf[KafkaTest])

  test("kafka") {
    import TestConfig._
    val topic = keyValueKafkaTopic
    assertTopic(topic) shouldBe topic
    produceMessages(topic, 3)
    consumeMessages(topic, 3) min 3
  }

  def produceMessages(topic: String, count: Int): Unit = {
    val producer = new KafkaProducer[String, String](TestConfig.kafkaProducerProperties)
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
}