package kafka

import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.Logger
import org.scalatest.{FunSuite, Matchers}

class KafkaStreamTest extends FunSuite with Matchers {
  val logger = Logger.getLogger(classOf[KafkaStreamTest])

  test("kafka stream") {
    val topic = TestConfig.valueKafkaTopic
    TestConfig.assertTopic(topic) shouldBe topic
    produceStream(topic, TestConfig.gettysburgAddress)
  }

  def produceStream(topic: String, words: Seq[String]): Unit = {
    val producer = new KafkaProducer[String, String](TestConfig.kafkaProducerProperties)
    words foreach { word => producer.send(new ProducerRecord[String, String](topic, word)) }
    producer.flush()
    producer.close(1000L, TimeUnit.MILLISECONDS)
  }
}