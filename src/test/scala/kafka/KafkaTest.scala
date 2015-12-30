package kafka

import java.util.Properties

import kafka.admin.AdminUtils
import kafka.consumer.{Consumer, ConsumerConfig}
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.StringDecoder
import kafka.utils.ZkUtils
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

class KafkaTest extends FunSuite {
  val kafkaTopic = "kv"

  test("kafka") {
    createKafkaTopic()
    assert(produceAndSendKafkaTopicMessages() == 3)
    assert(consumeKafkaTopicMessages() == 3)
  }

  private def createKafkaTopic(): Unit = {
    val zkClient = ZkUtils.createZkClient("localhost:2181", 3000, 3000)
    val zkUtils = ZkUtils(zkClient, isZkSecurityEnabled = false)
    val topicMetadata = AdminUtils.fetchTopicMetadataFromZk(kafkaTopic, zkUtils)
    topicMetadata.partitionsMetadata.foreach(println)
    if (topicMetadata.topic != kafkaTopic) {
      AdminUtils.createTopic(zkUtils, kafkaTopic, 1, 1, loadProperties("/kafka.producer.properties"))
      println(s"Topic ( $kafkaTopic ) created.")
    }
  }

  private def produceAndSendKafkaTopicMessages(): Int = {
    val config = new ProducerConfig(loadProperties("/kafka.producer.properties"))
    val producer = new Producer[String, String](config)
    val keyedMessages = ArrayBuffer[KeyedMessage[String, String]]()
    for (i <- 1 to 3) {
      val keyValue = i.toString
      keyedMessages += KeyedMessage[String, String](topic = kafkaTopic, key = keyValue, partKey = 0, message = keyValue)
    }
    producer.send(keyedMessages: _*)
    producer.close
    keyedMessages.size
  }

  private def consumeKafkaTopicMessages(): Int = {
    val config = new ConsumerConfig(loadProperties("/kafka.consumer.properties"))
    val connector = Consumer.create(config)
    val topicToStreams = connector.createMessageStreams(Map(kafkaTopic -> 1), new StringDecoder(), new StringDecoder())
    val streams = topicToStreams.get(kafkaTopic).get
    streams.foreach { s => s.foreach { m => println(s"topic: ${m.topic} key: ${m.key} value: ${m.message}") } }
    connector.shutdown // WARNING: Fails to shutdown.
    streams.size
  }

  private def loadProperties(file: String): Properties = {
    val properties = new Properties()
    properties.load(Source.fromInputStream(getClass.getResourceAsStream(file)).bufferedReader())
    properties
  }
}