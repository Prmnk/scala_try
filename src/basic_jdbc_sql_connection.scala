import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object basic_jdbc_sql_connection extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name"," basic app")
  sparkConf.set("spark.master","local[2]")
  
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  
  val conn_url = "jdbc:mysql://server_name/database_name"
  
  val mysql_properties = new java.util.Properties
  
  mysql_properties.setProperty("user", "username")
  
  mysql_properties.setProperty("password", "pwd")
  
 val orderdf =  spark.read.jdbc(conn_url,"table_name", mysql_properties)
 
 orderdf.show
  
}