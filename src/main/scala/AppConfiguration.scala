package insight.tweetsyn

import com.typesafe.config.ConfigFactory

object AppConfiguration {
  lazy val config = ConfigFactory.load()
}