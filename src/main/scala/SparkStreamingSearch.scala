package insight.twitterstat.spark

import com.redis._
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.{ElasticClient, ElasticsearchClientUri}
import com.typesafe.config.ConfigFactory
import kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.elasticsearch.common.settings.Settings

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

object AppConfiguration {
  lazy val config = ConfigFactory.load()
}

object ESConnection extends Serializable {
  val node1 = AppConfiguration.config.getString("elasticConfiguration.node1")
  val node2 = AppConfiguration.config.getString("elasticConfiguration.node2")
  val node3 = AppConfiguration.config.getString("elasticConfiguration.node3")
  val node4 = AppConfiguration.config.getString("elasticConfiguration.node4")
  val nodes = node1 + ":9300," + node2 + ":9300," + node3 + ":9300," + node4 + ":9300"

  var elasticUri = ElasticsearchClientUri(nodes,
    List((node1, 9300),
      (node2, 9300),
      (node3, 9300),
      (node4, 9300)))
  val settings = Settings.settingsBuilder().put("cluster.name", "olegtr").build
  lazy val elasticClient = ElasticClient.transport(settings, elasticUri)
}

object SparkStreamingSearch {

  def getSynonyms(redis: RedisClient, term: String) = {
    redis.zrange(term, 0, -1)
  }

  def main(args: Array[String]): Unit = {

    val brokers = AppConfiguration.config.getString("kafkaConfiguration.brokers")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    val conf = new SparkConf().setAppName("Load cached model")
    val ssc = new StreamingContext(conf, streaming.Seconds(10))

    val percolatorIndex = AppConfiguration.config.getString("elasticConfiguration.percolatorindex")

    val queriesTopic = Set(AppConfiguration.config.getString("kafkaConfiguration.queriesTopic"))
    val queriesStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, queriesTopic)
    val formattedQueries = queriesStream
      .map(_._2)
      .filter(query => query.contains(":"))
      .map { query => {
          val splitQuery = query.split(":")
          (splitQuery(0), splitQuery(1))
        }
      }
      .mapPartitions (queries => {
          val redis = new RedisClient(AppConfiguration.config.getString("redisConfiguration.host"), 6379)
          Future.sequence(queries.map {
            case (queryId, queryBody) => {
              val query = redis.zrange(queryBody) match {
                case Some(synonyms) => queryBody :: synonyms.filter(synonym => !synonym.contains(queryBody))
                case _ => List(queryBody)
              }
              ESConnection.elasticClient.execute {
                register id queryId into percolatorIndex query termsQuery("text", query:_*)
              }
            }
          }).await.map(response => response.getId)
        })
      .print

    ssc.start
    ssc.awaitTermination
  }
}