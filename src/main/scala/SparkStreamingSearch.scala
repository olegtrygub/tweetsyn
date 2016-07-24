package insight.tweetsyn

import com.redis._
import com.sksamuel.elastic4s.ElasticDsl._
import kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

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
      .filter {
        case (queryId, queryBody) => !queryId.isEmpty && !queryBody.isEmpty
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