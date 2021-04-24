import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Logger


object Dataframe_start extends App {
  
  //val spark = SparkSession.builder().appName("basic app").master("local[2]").getOrCreate()
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name"," basic app")
  sparkConf.set("spark.master","local[2]")
  
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  
  val orderDf = spark.read // action --> job
  .option("header",true) // get header as header
  .option("inferSchema",true) // not a good practice instead define explicitly  // action --> job
  .csv("C:/Users/Pramanik/Documents/Projects/spark/orders-201019-002101.csv") 

  //orderDf.show(5) // show data
  
  
  // sql like transformations
  val grpdf =  orderDf
  .repartition(4)  // wide transformation -- new stage  as number of partition = 4 ,tasks = 4
  .where("order_customer_id > 10000")
  .select("order_id","order_customer_id")
  .groupBy("order_customer_id") // wide transformation -- new stage
  .count()
  
  //action
  grpdf.show() // action --> job
  
  
  // throw log messages for debugging
  Logger.getLogger(getClass.getName).info("processing done by foo")
  
  //orderDf.printSchema()  //print schema details of the dataframe
  
  
  
  scala.io.StdIn.readLine()
  spark.stop()
  
}