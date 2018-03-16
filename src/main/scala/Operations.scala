//import java.sql.ResultSet

import com.datastax.driver.core.{ResultSet, Row, Session}

import scala.collection.JavaConverters._
class Operations {

  def createTable(session: Session): Unit = {
    session.execute("create table if not exists emp (id int, name text, city text,salary varint, phone varint, primary key(id,salary))")
    session.execute("create table if not exists emp1(id int, name text, city text,salary varint, phone varint, primary key(city))")
    session.execute("create index cityIndex on emp(city)")
  }

  def insertRecord(session: Session): Unit = {
    session.execute("insert into emp(id,name,city,salary,phone) values(1,'Ayush','Delhi',12000,9971870869)")
    session.execute("insert into emp(id,name,city,salary,phone) values(2,'Sam','New York',50000,9234567890)")
    session.execute("insert into emp(id,name,city,salary,phone) values(3,'Ram','Goa',12000,778899999)")
    session.execute("insert into emp(id,name,city,salary,phone) values(4,'Shyam','Delhi',12000,9993188803)")

    session.execute("insert into emp1(id,name,city,salary,phone) values(1,'Ayush','Delhi',12000,9971870869)")
    session.execute("insert into emp1(id,name,city,salary,phone) values(2,'Sam','New York',50000,9234567890)")
    session.execute("insert into emp1(id,name,city,salary,phone) values(3,'Ram','Goa',12000,778899999)")
    session.execute("insert into emp1(id,name,city,salary,phone) values(4,'Shyam','Delhi',12000,9993188803)")
  }

  def getAllRecord(session: Session): List[Row] = {
    session.execute("select * from emp").asScala.toList
  }

  def getRecordById(session: Session, emp_id: Int): List[Row] = {
    session.execute(s"select * from emp where id=$emp_id").asScala.toList
  }

  def updateRecord(session: Session, id: Int, salary: Int): List[Row] = {
    session.execute(s"update emp set city='chandigarh' where id=$id AND salary=$salary")
    getAllRecord(session)
  }

  def getRecordBySalary(session: Session, salary: Int, id: Int): ResultSet = {
    session.execute(s"select * from emp where id =$id AND salary > $salary ")
  }

  def getRecordByCity(session: Session, city: String): List[Row] = {
    session.execute(s"select * from emp where city ='$city'").asScala.toList
  }

  def deleteRecordByCity(session: Session, city: String): List[Row] = {
    session.execute(s"delete from emp1 where city = '$city'")
    session.execute("select * from emp1").asScala.toList
  }

}
