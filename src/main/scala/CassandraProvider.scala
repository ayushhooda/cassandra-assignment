import com.datastax.driver.core.{Cluster, Session}
import com.typesafe.config.{Config, ConfigFactory}

trait CassandraProvider {

  val config: Config = ConfigFactory.load()
  val cassandraKeyspace: String = config.getString("cassandra.keyspace")
  val cluster: Cluster = new Cluster.Builder().withClusterName("TestCluster").
    addContactPoints("localhost").build
  val session: Session = cluster.connect
  session.execute(s"CREATE KEYSPACE IF NOT EXISTS  $cassandraKeyspace WITH REPLICATION = " +
    s"{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
  session.execute(s"USE  $cassandraKeyspace")
  session
}
