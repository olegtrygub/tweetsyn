package insight.tweetsyn

import com.sksamuel.elastic4s.{ElasticClient, ElasticsearchClientUri}
import org.elasticsearch.common.settings.Settings

object ESConnection extends Serializable {
  val node1 = AppConfiguration.config.getString("elasticConfiguration.node1")
  val node2 = AppConfiguration.config.getString("elasticConfiguration.node2")
  val node3 = AppConfiguration.config.getString("elasticConfiguration.node3")
  val nodes = node1 + ":9300," + node2 + ":9300," + node3 + ":9300,"

  var elasticUri = ElasticsearchClientUri(nodes,
    List((node1, 9300),
      (node2, 9300),
      (node3, 9300)))
  val settings = Settings.settingsBuilder().put("cluster.name", "olegtr").build
  lazy val elasticClient = ElasticClient.transport(settings, elasticUri)
}