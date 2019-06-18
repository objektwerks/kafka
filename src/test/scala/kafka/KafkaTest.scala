package kafka

import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.scalatest.{FunSuite, Matchers}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class KafkaTest extends FunSuite with Matchers {
  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName)

  test("kafka") {
    import KafkaCommon._
    val topic = keyValueTopic
    assertTopic(topic) shouldBe true
    produceMessages(topic, 3)
    consumeMessages(topic, 3) min 3
  }

  def produceMessages(topic: String, count: Int): Unit = {
    val producer = new KafkaProducer[String, String](KafkaCommon.kafkaProducerProperties)
    for (i <- 1 to count) {
      val key = i.toString
      val value = key
      val record = new ProducerRecord[String, String](topic, key, value)
      val metadata = producer.send(record).get()
      logger.info(s"Producer -> topic: ${metadata.topic} partition: ${metadata.partition} offset: ${metadata.offset}")
      logger.info(s"Producer -> key: ${record.key} value: ${record.value}")
    }
    producer.flush()
    producer.close()
  }

  def consumeMessages(topic: String, retries: Int): Int = {
    val consumer = new KafkaConsumer[String, String](KafkaCommon.kafkaConsumerProperties)
    consumer.subscribe(List(topic).asJava)
    val count = new AtomicInteger()
    for (i <- 1 to retries) {
      logger.info(s"Consumer -> polling attempt $i ...")
      val records = consumer.poll(Duration.ofMillis(100L))
      logger.info(s"Consumer -> ${records.count} records polled.")
      val iterator = records.iterator()
      while (iterator.hasNext) {
        val record = iterator.next
        logger.info(s"Consumer -> topic: ${record.topic} partition: ${record.partition} offset: ${record.offset}")
        logger.info(s"Consumer -> key: ${record.key} value: ${record.value}")
        count.incrementAndGet()
      }
    }
    consumer.commitAsync()
    consumer.close()
    count.get
  }
}