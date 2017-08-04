package kafka

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder, KTable}
import org.apache.log4j.Logger
import org.scalatest.{FunSuite, Matchers}

import scala.collection.JavaConverters._

class KafkaStreamTest extends FunSuite with Matchers {
  val logger = Logger.getLogger(classOf[KafkaStreamTest])

  test("kafka stream") {
    import TestConfig._
    val sourceTopic = valueKafkaTopic
    val sinkTopic = countKafkaTopic
    assertTopic(sourceTopic) shouldBe sourceTopic
    assertTopic(sinkTopic) shouldBe sinkTopic
    produceStream(sourceTopic, gettysburgAddress)
    processStream(sourceTopic, sinkTopic, kafkaStreamProperties)
    consumeMessages(sinkTopic, 3)
  }

  def produceStream(topic: String, lines: Seq[String]): Unit = {
    val producer = new KafkaProducer[String, Long](TestConfig.kafkaProducerProperties)
    lines foreach { line =>
      val record = new ProducerRecord[String, Long](topic, line, line.length)
      val metadata = producer.send(record).get()
      logger.info(s"Producer -> topic: ${metadata.topic} partition: ${metadata.partition} offset: ${metadata.offset}")
      logger.info(s"Producer -> key: $line value: ${line.length}")
    }
    producer.flush()
    producer.close(1000L, TimeUnit.MILLISECONDS)
  }

  def processStream(sourceTopic: String, sinkTopic: String, properties: Properties): Unit = {
    val builder: KStreamBuilder = new KStreamBuilder()
    val lines: KStream[String, Long] = builder.stream(sourceTopic)
    val counts: KTable[String, Long] = lines.flatMap(line => line.toLowerCase.split("\\W+").toIterable.asJava).groupByKey().count()
    counts.to(sinkTopic)
    val streams: KafkaStreams = new KafkaStreams(builder, properties)
    streams.start()
    sys.addShutdownHook(new Thread(() => { streams.close(10, TimeUnit.SECONDS) }))
  }
}