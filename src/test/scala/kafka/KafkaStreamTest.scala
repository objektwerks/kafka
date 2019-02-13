package kafka

import java.time.Duration
import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.scalatest.{FunSuite, Matchers}

import scala.collection.JavaConverters._

class KafkaStreamTest extends FunSuite with Matchers {
  val logger = Logger(classOf[KafkaStreamTest])

  test("kafka stream") {
    import TestConfig._
    assertTopic(wordTopic) shouldBe true
    assertTopic(wordCountTopic) shouldBe true
    produceStream(wordTopic, gettysburgAddress)
    WordCount.count(wordTopic, wordCountTopic, kafkaStreamProperties)
    consumeMessages(wordCountTopic, 3)
  }

  def produceStream(topic: String, lines: Seq[String]): Unit = {
    val producer = new KafkaProducer[String, String](TestConfig.kafkaProducerProperties)
    lines foreach { line =>
      val record = new ProducerRecord[String, String](topic, line, line)
      val metadata = producer.send(record).get()
      logger.info(s"Producer -> topic: ${metadata.topic} partition: ${metadata.partition} offset: ${metadata.offset}")
      logger.info(s"Producer -> key: $line value: ${line.length}")
    }
    producer.flush()
    producer.close(1000L, TimeUnit.MILLISECONDS)
  }

/*  NOTE: Failed to resolve KStream/KTable type errors.

  def processStream(sourceTopic: String, sinkTopic: String, properties: Properties): Unit = {
    val builder: KStreamBuilder = new KStreamBuilder()
    val lines: KStream[String, String] = builder.stream(sourceTopic)
    val counts: KTable[String, Long] = lines.flatMapValues(line => line.toLowerCase.split("\\W+").toIterable.asJava).groupBy((_, word) => word).count()
    counts.to(sinkTopic)
    val streams: KafkaStreams = new KafkaStreams(builder, properties)
    streams.start()
    sys.addShutdownHook(new Thread(() => { streams.close(10, TimeUnit.SECONDS) }))
  }
*/

  def consumeMessages(topic: String, retries: Int): Unit = {
    val consumer = new KafkaConsumer[String, String](TestConfig.kafkaConsumerWordCountProperties)
    consumer.subscribe(List(topic).asJava)
    for (i <- 1 to retries) {
      logger.info(s"Consumer -> polling attempt $i ...")
      val records = consumer.poll(Duration.ofMillis(100L))
      logger.info(s"Consumer -> ${records.count} records polled.")
      val iterator = records.iterator()
      while (iterator.hasNext) {
        val record = iterator.next
        logger.info(s"Consumer -> topic: ${record.topic} partition: ${record.partition} offset: ${record.offset}")
        logger.info(s"Consumer -> key: ${record.key} value: ${record.value}")
      }
    }
    consumer.close()
  }
}