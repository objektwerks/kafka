package kafka

import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.Logger
import org.scalatest.{FunSuite, Matchers}

class KafkaStreamTest extends FunSuite with Matchers {
  val logger = Logger.getLogger(classOf[KafkaStreamTest])

  test("kafka stream") {
    import TestConfig._
    val sourceTopic = wordTopic
    val sinkTopic = countTopic
    assertTopic(sourceTopic) shouldBe sourceTopic
    assertTopic(sinkTopic) shouldBe sinkTopic
    produceStream(sourceTopic, gettysburgAddress)
    WordCount.count(sourceTopic, sinkTopic, kafkaStreamProperties)
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

/*  Currently unable to correct type errors with this line of code:

      lines.flatMap(line => line.toLowerCase.split("\\W+").toIterable.asJava).groupByKey().count()

  def processStream(sourceTopic: String, sinkTopic: String, properties: Properties): Unit = {
    val builder: KStreamBuilder = new KStreamBuilder()
    val lines: KStream[String, Long] = builder.stream(sourceTopic)
    val counts: KTable[String, Long] = lines.flatMap(line => line.toLowerCase.split("\\W+").toIterable.asJava).groupByKey().count()
    counts.to(sinkTopic)
    val streams: KafkaStreams = new KafkaStreams(builder, properties)
    streams.start()
    sys.addShutdownHook(new Thread(() => { streams.close(10, TimeUnit.SECONDS) }))
  }*/
}