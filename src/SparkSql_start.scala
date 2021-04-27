import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object SparkSql_start extends App {
   val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name"," spark_sql)app")
  sparkConf.set("spark.master","local[2]")
  
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  
  val orderDf = spark.read
  .option("header",true) 
  .option("inferSchema",true) 
  .csv("C:/Users/Pramanik/Documents/Projects/spark/orders-201019-002101.csv") 

  orderDf.createOrReplaceTempView("orders")
  
  val resultdf = spark.sql("Select * from orders order by 1 desc limit 10")
  
  resultdf.show
  
  spark.stop
  
}