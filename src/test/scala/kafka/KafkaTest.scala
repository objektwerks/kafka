package kafka

import java.util.Properties

import kafka.admin.AdminUtils
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

class KafkaTest extends FunSuite {
  val kafkaTopic = "ratings"

  test("kafka") {
    createKafkaTopic()
    produceAndSendKafkaTopicMessages()
    consumeKafkaTopicMessages()
  }

  private def createKafkaTopic(): Unit = {
    val zkClient = new ZkClient("localhost:2181", 3000, 3000, ZKStringSerializer)
    val metadata = AdminUtils.fetchTopicMetadataFromZk(kafkaTopic, zkClient)
    metadata.partitionsMetadata.foreach(println)
    if (metadata.topic != kafkaTopic) {
      AdminUtils.createTopic(zkClient, kafkaTopic, 1, 1)
    }
  }

  private def produceAndSendKafkaTopicMessages(): Seq[(String, String, String, String)] = {
    val props = new Properties
    props.load(Source.fromInputStream(getClass.getResourceAsStream("/kafka.properties")).bufferedReader())
    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)
    val source = Source.fromInputStream(getClass.getResourceAsStream("/ratings")).getLines.toSeq
    val ratings = ArrayBuffer[(String, String, String, String)]()
    val keyedMessages = ArrayBuffer[KeyedMessage[String, String]]()
    source foreach { line =>
      val fields = line.split(",")
      val rating = (fields(0), fields(1), fields(2), fields(3))
      ratings += rating
      keyedMessages += KeyedMessage[String, String](topic = kafkaTopic, key = line, partKey = 0, message = line)
    }
    producer.send(keyedMessages: _*)
    ratings
  }

  private def consumeKafkaTopicMessages(): Unit = {
    val props = new Properties
    props.load(Source.fromInputStream(getClass.getResourceAsStream("/kafka.properties")).bufferedReader())
    val consumer = new KafkaConsumer(props)
    consumer.subscribe(kafkaTopic)
    val isRunning = true
    while(isRunning) {
      consumer.poll(100)
    }
    consumer.close()
  }
}