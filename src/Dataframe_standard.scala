import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger


object Dataframe_standard extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf
  
  sparkConf.set("spark.app.name","standard_ingestion").set("spark.master","local[*]")
  
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  
  val orderdf = spark.read
 // .format("json") --> defualt format is parquet hence no need to specify
  .option("path","C:/Users/Pramanik/Documents/Projects/spark/users-201019-002101.parquet")
  .option("mode","DROPMALFORMED") // 1. default--> Permissive - sets bad data to NULL 2. DropMalformed ignores bad data 3. Failfast - raises exception
  .load
   
  orderdf.printSchema()
  
  orderdf.show(false) // truncate = false, otherwise truncates after some character
  
  spark.stop()
  
}