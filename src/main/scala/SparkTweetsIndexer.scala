package insight.twitterstat.spark

import com.redis._
import com.sksamuel.elastic4s.ElasticDsl._
import com.typesafe.config.ConfigFactory
import kafka.serializer.StringDecoder
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf}
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.elasticsearch.action.percolate.PercolateResponse
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.spark.rdd.EsSpark
import twitter4j.TwitterObjectFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, _}
import scala.concurrent.duration._

case class TweetInformation(id: Long, text: String, username: String, hashtags: String)

object SparkTweetsIndexer {

  def indexWithTopic(index:String, topic:String) = index + "/" + topic

  def main(args: Array[String]): Unit = {

    val appConfiguration = ConfigFactory.load()
    val brokers = appConfiguration.getString("kafkaConfiguration.brokers")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    val conf = new SparkConf().setAppName("Load cached model")
    val ssc = new StreamingContext(conf, streaming.Seconds(5))

    val tweetsTopic = Set(appConfiguration.getString("kafkaConfiguration.tweetsTopic"))
    val twitterStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, tweetsTopic)

    val index = appConfiguration.getString("elasticConfiguration.index")
    val topic = appConfiguration.getString("elasticConfiguration.topic")
    val percolatorIndex = appConfiguration.getString("elasticConfiguration.percolatorindex")

    val jobConfiguration = new JobConf(ssc.sparkContext.hadoopConfiguration)
    jobConfiguration.set("mapred.output.format.class", "org.elasticsearch.hadoop.mr.EsOutputFormat")
    jobConfiguration.setOutputCommitter(classOf[FileOutputCommitter])
    jobConfiguration.set(ConfigurationOptions.ES_RESOURCE, indexWithTopic(index, topic))
    jobConfiguration.set(ConfigurationOptions.ES_NODES, appConfiguration.getString("elasticConfiguration.node1"))
    FileOutputFormat.setOutputPath(jobConfiguration, new Path(appConfiguration.getString("elasticConfiguration.path")))

    val tweetsForIndexing = twitterStream
      .map(_._2)
      .map(json => TwitterObjectFactory.createStatus(json))
      .map {
        status => new TweetInformation(status.getId,
          status.getText,
          status.getUser.getName,
          status.getHashtagEntities.map(tag => tag.getText).mkString("# "))
      }

    tweetsForIndexing.foreachRDD {
      tweetsRDD => EsSpark.saveToEs(tweetsRDD, indexWithTopic(index, topic))
    }

    tweetsForIndexing.mapPartitions[(Future[PercolateResponse], TweetInformation)] {
      tweetsInformation => tweetsInformation.map (tweetInformation => {
        (ESConnection.elasticClient.execute {
          percolate in indexWithTopic(percolatorIndex, topic) doc "text" -> tweetInformation.text
        }, tweetInformation)
      })
    }.mapPartitions[(String, TweetInformation)] {
      partitionFutures => {
        val input = partitionFutures.toList
        val futures: Future[Iterator[PercolateResponse]] = Future.sequence(input.map(_._1).iterator)
        val responses = Await.result(futures, 5 seconds).zip(input.map(_._2).iterator)
        responses.flatMap {
          case (response, tweetInformation) => response.getMatches.map(responseMatch => (responseMatch.getId.toString, tweetInformation))
        }
      }
    }.groupByKey.map {
      case (queryId, tweetsInformation) => {
        def redis = new RedisClient(AppConfiguration.config.getString("redisConfiguration.host"), 6379)
        tweetsInformation.foreach {
         tweetInformation => redis.publish(queryId, tweetInformation.text)
        }
        queryId
      }
    }.print

    ssc.start
    ssc.awaitTermination
  }
}