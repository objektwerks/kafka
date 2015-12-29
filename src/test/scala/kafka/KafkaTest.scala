package kafka

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.scalatest.FunSuite

import scala.collection.JavaConverters._
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
    val consumer = new KafkaConsumer[String, String](loadProperties("/kafka.consumer.properties"))
    consumer.subscribe(List(kafkaTopic).asJava)
  }

  private def loadProperties(file: String): Properties = {
    val properties = new Properties()
    properties.load(Source.fromInputStream(getClass.getResourceAsStream(file)).bufferedReader())
    properties
  }
}