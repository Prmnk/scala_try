import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

object SparkSQL_pivot extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name"," spark_sql_pivot)app")
  sparkConf.set("spark.master","local[2]")
  
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
   
  val loginfo = spark.read
   .format("csv")
   .option("header",true)
   .option("path","C:/Users/Pramanik/Documents/Projects/spark/biglog-201105-152517.txt")
   .load()
   
  loginfo.createOrReplaceTempView("loginfo")
  
  spark.sql("Select level, date_format(datetime, 'MMMM') as dt , count(*) from loginfo group by level , date_format(datetime, 'MMMM') limit 10").show()
  
  
}