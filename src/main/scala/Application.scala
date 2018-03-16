import com.datastax.driver.core.ConsistencyLevel

object Application extends App with CassandraProvider {

  session.getCluster.getConfiguration.getQueryOptions.setConsistencyLevel(ConsistencyLevel.QUORUM)
  val obj = new Operations
  obj.createTable(session)
  obj.insertRecord(session)

  // Q1
  val record1 = obj.getRecordById(session, 1)
  record1.foreach(println(_))

  // Q2
  val record2 = obj.updateRecord(session, 1, 80000)
  record2.foreach(println(_))

  // Q3 Fetch record with salary greater than 30k
  val record3 = obj.getRecordBySalary(session, 30000, 2)
  record3.forEach(println(_))

  // Q4 Get records on basis of city
  val record4 = obj.getRecordByCity(session, "Goa")
  record4.foreach(println(_))

  // Q5 Delete record with city
  val record5 = obj.deleteRecordByCity(session, "Delhi")
  record5.foreach(println(_))


  if (!session.isClosed) {
    session.close()
  }

}
