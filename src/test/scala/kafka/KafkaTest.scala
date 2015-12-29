package kafka

import java.util.Properties

import kafka.admin.AdminUtils
import kafka.consumer.{Consumer, ConsumerConfig}
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer._
import kafka.utils.ZkUtils
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

class KafkaTest extends FunSuite {
  val kafkaTopic = "kv"

  test("kafka") {
    createKafkaTopic()
    produceAndSendKafkaTopicMessages()
    consumeKafkaTopicMessages()
  }

  private def createKafkaTopic(): Unit = {
    val zkClient = ZkUtils.createZkClient("localhost:2181", 3000, 3000)
    val zkUtils = ZkUtils(zkClient, isZkSecurityEnabled = false)
    val topicMetadata = AdminUtils.fetchTopicMetadataFromZk(kafkaTopic, zkUtils)
    topicMetadata.partitionsMetadata.foreach(println)
    if (topicMetadata.topic != kafkaTopic) {
      AdminUtils.createTopic(zkUtils, kafkaTopic, 1, 1, loadProperties("/kafka.producer.properties"))
    }
  }

  private def produceAndSendKafkaTopicMessages(): Unit = {
    val config = new ProducerConfig(loadProperties("/kafka.producer.properties"))
    val producer = new Producer[String, String](config)
    val keyedMessages = ArrayBuffer[KeyedMessage[String, String]]()
    for (i <- 1 to 10) {
      val keyValue = i.toString
      keyedMessages += KeyedMessage[String, String](topic = kafkaTopic, key = keyValue, partKey = 0, message = keyValue)
    }
    producer.send(keyedMessages: _*)
    producer.close()
  }

  private def consumeKafkaTopicMessages(): Unit = {
    val config = new ConsumerConfig(loadProperties("/kafka.consumer.properties"))
    val connector = Consumer.create(config)
    val streams = connector.createMessageStreams(Map(kafkaTopic -> 1), new StringDecoder(), new StringDecoder())

  }

  private def loadProperties(file: String): Properties = {
    val properties = new Properties()
    properties.load(Source.fromInputStream(getClass.getResourceAsStream(file)).bufferedReader())
    properties
  }
}