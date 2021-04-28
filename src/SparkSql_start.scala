import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._ // must for using column/col



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
  
  orderDf.select("order_id","order_date").show()
  
  import spark.implicits._
  
  orderDf.select(column("order_id"),col("order_date"), $"order_customer_id",'order_status).show()
  
  resultdf.show
  /*
  resultdf.write
  .format("csv")
  //.mode(SaveMode.Overwrite) 
  //.option("path","C:/Users/Pramanik/Documents/Projects/spark/output_par_db")   
  .partitionBy("order_status")
  .bucketBy(4,"order_id")
  .sortBy("order_customer_id")
  .saveAsTable("order_db.order_tbl")
  
  spark.catalog.listTables("order_db").show()
  */
  spark.stop
  
}