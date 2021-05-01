import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._


object Dataframe_joins extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name"," spark_sql)app")
  sparkConf.set("spark.master","local[2]")
  
   val spark = SparkSession.builder().config(sparkConf).getOrCreate()
   
   val orderd = spark.read
   .format("csv")
   .option("header",true)
   .option("path","C:/Users/Pramanik/Documents/Projects/spark/orders_samecol.csv")
   .load()
   
   val cust = spark.read
   .format("csv")
   .option("inferschema",true)
   .option("header",true)
   .option("path","C:/Users/Pramanik/Documents/Projects/spark/customers.csv")
   .load()
   
  // cust.show()
  // orderd.show()
   
   spark.sql("SET spark.sql.autoBroadcastJoinThreshold = -1") // to avoid broadcast join - otherwise system automatically optimizes
   
   val order_new = orderd.withColumnRenamed("customer_id","cust_id")
   
   val joincondition = order_new.col("cust_id")===cust.col("customer_id")
   val jointype = "left"
   val joineddf = order_new.join(broadcast(cust),joincondition,jointype).sort(order_new.col("order_id")) // broadcast for broad cast join
   .drop(order_new.col("cust_id"))
   .withColumn("order_id",expr("coalesce(order_id,1)")) // replace NULL with some value
  // .select("order_id","customer_id")  // orderd = left, cust = right
   
   joineddf.show()
   
   
   spark.stop
   
   
}