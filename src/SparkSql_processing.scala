import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

object SparkSql_processing extends App {
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name"," spark_sql)app")
  sparkConf.set("spark.master","local[2]")
  
  val spark = SparkSession.builder().config(sparkConf).enableHiveSupport.getOrCreate()
  
val lst = List(  
(1,"2013-07-25 00:00:00.0",11599,"CLOSED")
,(2,"2013-07-25 00:00:00.0",256,"PENDING_PAYMENT")
,(3,"2013-07-24 00:00:00.0",12111,"COMPLETE")
,(4,"2013-07-25 00:00:00.0",8827,"CLOSED")
,(5,"2013-07-25 00:00:00.0",11318,"COMPLETE")
,(6,"2013-07-25 00:00:00.0",11318,"COMPLETE")
 ) 
 
 val orderdf = spark.createDataFrame(lst).toDF("order_id","orderdate","cust_id","status")
 
 val orderdf1 = orderdf.withColumn("orderdate",unix_timestamp(col("orderdate").cast(DateType)))
 .withColumn("newid", monotonically_increasing_id)
 .dropDuplicates("cust_id")
 .drop("order_id")
 .sort("orderdate")
 
 orderdf1.show()
 
 spark.stop
}