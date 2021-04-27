import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object SparkSql_start extends App {
   val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name"," spark_sql)app")
  sparkConf.set("spark.master","local[2]")
  
  val spark = SparkSession.builder().config(sparkConf).enableHiveSupport.getOrCreate()
  
  val orderDf = spark.read
  .option("header",true) 
  .option("inferSchema",true) 
  .csv("C:/Users/Pramanik/Documents/Projects/spark/orders-201019-002101.csv") 
  
  spark.sql("create database if not exists order_db")

  orderDf.createOrReplaceTempView("orders")
  
  val resultdf = spark.sql("Select * from orders order by 1 desc limit 1000")
  
  resultdf.show
  
  resultdf.write
  .format("csv")
  //.mode(SaveMode.Overwrite) 
  //.option("path","C:/Users/Pramanik/Documents/Projects/spark/output_par_db")   
  .partitionBy("order_status")
  .bucketBy(4,"order_id")
  .sortBy("order_customer_id")
  .saveAsTable("order_db.order_tbl")
  
  spark.catalog.listTables("order_db").show()
  
  spark.stop
  
}