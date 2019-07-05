package kafka

import java.time.Duration
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.Logger

import scala.collection.JavaConverters._
import scala.io.Source

object KafkaCommon {
  val kafkaConsumerProperties = loadProperties("/kafka-consumer.properties")
  val kafkaProducerProperties = loadProperties("/kafka-producer.properties")
  val adminClientProperties = new Properties()
  adminClientProperties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  def loadProperties(file: String): Properties = {
    val properties = new Properties()
    properties.load(Source.fromInputStream(getClass.getResourceAsStream(file)).bufferedReader())
    properties
  }

  def assertTopic(topic: String): Boolean = {
    val adminClient = AdminClient.create(adminClientProperties)
    val newTopic = new NewTopic(topic, 1, 1.toShort)
    val createTopicResult = adminClient.createTopics(List(newTopic).asJavaCollection)
    createTopicResult.values().containsKey(topic)
  }

  def countMessages(topic: String)(implicit logger: Logger): Int = {
    val consumer = new KafkaConsumer[String, String](kafkaConsumerProperties)
    consumer.subscribe(List(topic).asJava)
    val count = new AtomicInteger()
    for (_ <- 1 to 2) {
      val records = consumer.poll(Duration.ofMillis(100L))
      records.iterator.asScala.foreach { _ => count.incrementAndGet }
    }
    consumer.close()
    logger.info(s"+++ Consumer -> record count is ${count.get}")
    count.get
  }
}