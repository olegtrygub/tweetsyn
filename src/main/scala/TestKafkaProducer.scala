import java.util.Properties
import scala.io.Source
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}
import twitter4j.{TwitterObjectFactory, Status, FilterQuery}

object TwitterKafkaProducer extends App {

  val appConfiguration = ConfigFactory.load()
  val brokers = appConfiguration.getString("kafkaConfiguration.brokers")
  val topic = appConfiguration.getString("kafkaConfiguration.tweetsTopic")

  val kafkaProducer = {
    val props = new Properties()

    props.put("producer.type", "async")
    props.put("bootstrap.servers", brokers)
    props.put("acks", "all")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    new KafkaProducer[String, String](props)
  }

  val filePath = args(0)
  val speed = args(1)

  for (line <-Source.fromFile(filePath).getLines) {
    kafkaProducer.send(new ProducerRecord[String, String](topic, TwitterObjectFactory.getRawJSON(line)))
  }

}