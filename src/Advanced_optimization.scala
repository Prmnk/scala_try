import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object Advanced_optimization extends App {
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name"," basic app")
  sparkConf.set("spark.master","local[2]")
  
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  
  val orderDf = spark.read 
  .option("header",true) 
  .option("inferSchema",true) 
  .csv("C:/Users/Pramanik/Documents/Projects/spark/order_data-201025-223502.csv") 
  
  val cus = spark.read
  .option("header",true) 
  .option("inferSchema",true) 
  .csv("C:/Users/Pramanik/Documents/Projects/spark/customers.csv")
  
  //orderDf.show()
  
 // cus.show()
  
  //spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
  
  // default it will do a broadcast join
  val joindf = cus.join(orderDf,cus("customer_id")=== orderDf("CustomerID"))
  
  joindf.show()
  
  spark.stop
  
}