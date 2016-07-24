package insight.tweetsyn

import java.util.Properties

import com.typesafe.config.ConfigFactory

import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}

import twitter4j._
import twitter4j.FilterQuery
import twitter4j.Status
import twitter4j.conf.ConfigurationBuilder
import twitter4j.conf.Configuration
import twitter4j.TwitterObjectFactory

object TwitterStream {

  private val appConfiguration = ConfigFactory.load()

  private val getTwitterConfiguration: Configuration = {
    val twitterConf = new ConfigurationBuilder()
      .setDebugEnabled(true)
      .setOAuthConsumerKey(appConfiguration.getString("twitterConfiguration.consumerKey"))
      .setOAuthConsumerSecret(appConfiguration.getString("twitterConfiguration.consumerSecretKey"))
      .setOAuthAccessToken(appConfiguration.getString("twitterConfiguration.accessToken"))
      .setOAuthAccessTokenSecret(appConfiguration.getString("twitterConfiguration.accessTokenSecret"))
      .setJSONStoreEnabled(true)
      .build()
    twitterConf
  }

  def getStream = new TwitterStreamFactory(getTwitterConfiguration).getInstance()

  class OnTweetPosted(handler: Status => Unit) extends StatusListener {

    override def onStatus(status: Status): Unit = handler(status)
    override def onException(ex: Exception): Unit = throw ex

    // no-op for the following events
    override def onStallWarning(warning: StallWarning): Unit = {}
    override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {}
    override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = {}
    override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = {}
  }
}

object TwitterKafkaProducer extends App {

  val filterUsOnly = new FilterQuery().locations(Array(
    Array(-126.562500,30.448674),
    Array(-61.171875,44.087585))).language(Array("en"))

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

  val twitterStream = TwitterStream.getStream
  twitterStream.addListener(new TwitterStream.OnTweetPosted(produce))
  twitterStream.filter(filterUsOnly)

  private def produce(status: Status): Unit = {
    kafkaProducer.send(new ProducerRecord[String, String](topic, TwitterObjectFactory.getRawJSON(status)))
  }

}
