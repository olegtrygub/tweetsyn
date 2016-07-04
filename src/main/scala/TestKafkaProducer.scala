package insight.twitterstat.kafka

import java.util.Properties
import scala.io.Source
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}

object TеstKafkaProducer extends App {

  val appConfiguration = ConfigFactory.load()
  val brokers = appConfiguration.getString("kafkaConfiguration.brokers")
  val topic = appConfiguration.getString("kafkaConfiguration.tweetsTopic")

  val kafkaProducer = {
    val props = new Properties()

    props.put("bootstrap.servers", brokers)
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    new KafkaProducer[String, String](props)
  }

  val filePath = args(0)
  val speed = args(1).toInt
  var cnt = 0
  var start = System.nanoTime
  val million = 1000000
  val billion = 1000 * million
  for (line <-Source.fromFile(filePath).getLines) {
    kafkaProducer.send(new ProducerRecord[String, String](topic, line))
    cnt += 1
    if (cnt >= speed) {
      val elapsed = System.nanoTime - start
      if (elapsed / billion < 1) {
        Thread.sleep((billion - elapsed) / million)
      }
      cnt = 0
      start = System.nanoTime
    }
  }
}