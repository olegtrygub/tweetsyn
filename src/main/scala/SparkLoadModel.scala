package insight.tweetsyn

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.redis._

object SparkLoadModel {

  def main(args: Array[String]): Unit = {
    val appConfiguration = ConfigFactory.load
    val conf = new SparkConf().setAppName("Load cached model")
    val sc = new SparkContext(conf)
    val textCachedModel = sc.textFile("/home/ubuntu/Downloads/synonyms")
    val redis = new RedisClient(appConfiguration.getString("redisConfiguration.host"), 6379)

    textCachedModel
      .map(string => string.toLowerCase)
      .map(synonymsList => {
        val semicolonPos = synonymsList.indexOf(";")
        val word = synonymsList.substring(0, semicolonPos)
        val synonyms = synonymsList.substring(semicolonPos+1).split(",")
        (word, synonyms)
      })
      .foreach {
        case (word, synonyms) => {
          synonyms.foreach { synonym => redis.zadd(word, 1.0, synonym) }
        }
      }
  }
}
