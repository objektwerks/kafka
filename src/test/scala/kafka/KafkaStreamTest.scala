package kafka

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder, KTable}
import org.apache.log4j.Logger
import org.scalatest.{FunSuite, Matchers}

import scala.collection.JavaConverters._

class KafkaStreamTest extends FunSuite with Matchers {
  val logger = Logger.getLogger(classOf[KafkaStreamTest])

  test("kafka stream") {
    import TestConfig._
    assertTopic(wordTopic) shouldBe wordTopic
    assertTopic(wordCountTopic) shouldBe wordCountTopic
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

  def processStream(sourceTopic: String, sinkTopic: String, properties: Properties): Unit = {
    val builder: KStreamBuilder = new KStreamBuilder()
    val lines: KStream[String, String] = builder.stream(sourceTopic)
    val counts: KTable[String, String] = lines.flatMapValues(line => line.toLowerCase.split("\\W+").toIterable.asJava).groupByKey().count()
    counts.to(sinkTopic)
    val streams: KafkaStreams = new KafkaStreams(builder, properties)
    streams.start()
    sys.addShutdownHook(new Thread(() => { streams.close(10, TimeUnit.SECONDS) }))
  }
}