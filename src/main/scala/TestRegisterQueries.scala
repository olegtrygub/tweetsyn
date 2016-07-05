package insight.twitterstat.spark

import com.redis._
import com.sksamuel.elastic4s.ElasticDsl._

object TestRegisterQueries extends App {
  val redis = new RedisClient(AppConfiguration.config.getString("redisConfiguration.host"), 6379)
  val percolatorIndex = AppConfiguration.config.getString("elasticConfiguration.percolatorindex")

  val allKeys = redis.keys("*")

  for {
    keys <- allKeys
    key <- keys
    query <- key
  } {
    val e = ESConnection.elasticClient.execute {
      register id query into percolatorIndex query termQuery("text", query)
    }.await
  }
}
