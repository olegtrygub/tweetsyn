package insight.tweetsyn

import scala.collection.JavaConverters._

import java.util.Properties
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.KafkaConsumer

object KafkaConsumerApp extends App {

  val props = new Properties()

  val appConfiguration = ConfigFactory.load()
  val brokers = appConfiguration.getString("kafkaConfiguration.brokers")
  val topic = appConfiguration.getString("kafkaConfiguration.topic")

  props.put("producer.type", "async")
  props.put("bootstrap.servers", brokers)
  props.put("acks", "all")
  props.put("group.id", appConfiguration.getString("kafkaConfiguration.group"))
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  props.put("session.timeout.ms", "30000")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

  val consumer = new KafkaConsumer[String, String](props)
  val scalaList = List(topic)
  consumer.subscribe(scalaList.asJava)

  while (true) {
    val records = consumer.poll(1000)
    print(records.count())
  }
}

