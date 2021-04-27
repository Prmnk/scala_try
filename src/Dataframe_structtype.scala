

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.TimestampType

//case class order(order_id: Int, order_date: Timestamp, customer_id: Int, order_status : String)

object Dataframe_structtype extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf
  
  sparkConf.set("spark.app.name","explicit_schema").set("spark.master","local[*]")
  
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  
  // defining the schema explicitly for a dataframe
  
  // way #1
  val orderschema = StructType(List( // with spark datatypes
      StructField("orderid",IntegerType),
      StructField("orderdate",TimestampType),
      StructField("customerid",IntegerType),
      StructField("status",StringType)
  ))
  
  val windowdataSchema = StructType(List(       
      StructField("Country",StringType),       
      StructField("weeknum",IntegerType),       
      StructField("numinvoices",IntegerType),       
      StructField("totalquantity",IntegerType),       
      StructField("invoicevalue",DoubleType)       ))

  
  // way #2 
  //val schemaddl = "orderid Int, orderdate String,custid Int, ordstatus String " // with scala datatypes
  
  val orderdf = spark.read
  .format("csv") 
  .schema(windowdataSchema) // explicit schema 
  .option("path","C:/Users/Pramanik/Documents/Projects/spark/windowdata-201021-002706.csv")
  .option("mode","DROPMALFORMED") 
  .load
  
  
  val  newdf = orderdf.repartition(4)
  
  println(newdf.rdd.getNumPartitions)
  
  // write data to a file and partition it
  
   newdf.write 
  // .format("json") default parquet
   .partitionBy("Country", "weeknum") 
   //.bucketBy(4,"numinvoices")
   .option("maxRecordsPerFile",20)
   .mode(SaveMode.Overwrite)   // Append
   .option("path","C:/Users/Pramanik/Documents/Projects/spark/output_par")   
   .save()
   
  orderdf.printSchema()
}