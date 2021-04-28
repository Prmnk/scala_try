import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object SparkSQL_aggregations extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name"," spark_sql)app")
  sparkConf.set("spark.master","local[2]")
  
   val spark = SparkSession.builder().config(sparkConf).getOrCreate()
   
   val orderd = spark.read
   .format("csv")
   .option("inferschema",true)
   .option("header",true)
   .option("path","C:/Users/Pramanik/Documents/Projects/spark/order_data-201025-223502.csv")
   .load()
   
   //////// Basic Aggregations
   // column object expession
   orderd.select(
   count("*").as("RowCnt"),
   sum("Quantity").as("Tot_Quantity"),
   avg("UnitPrice").as("Avg_UP"),
   countDistinct("InvoiceNo").as("CountDistinct")
   ).show()
   
   
   // string expression
   orderd.selectExpr(
   "count(*)  as RowCnt",
   "sum(Quantity) as Tot_Quantity",
   "avg(UnitPrice) as Avg_UP",
   "count(Distinct(InvoiceNo)) as CountDistinct"
   ).show()
   
   
   // Spark sql style
   orderd.createOrReplaceTempView("order")
   
   spark.sql("Select count(*) as RowCnt, sum(Quantity) as Tot_Quantity,avg(UnitPrice) as Avg_UP, count(Distinct(InvoiceNo)) as CountDistinct from order").show()
   
   
   // More grouping aggregations
   
   orderd.groupBy("Country", "InvoiceNo")
   .agg(sum("Quantity").as("Tot_Quant"),
       sum(expr("Quantity * UnitPrice")).as("InvoiceValue")
       ).show()
       
   orderd.groupBy("Country", "InvoiceNo")
   .agg(expr("sum(Quantity) as Tot_Quant"),
       expr("sum( Quantity * UnitPrice) as InvoiceValue")
       ).show()
       
   orderd.createOrReplaceTempView("orderde")
   
   spark.sql("Select  Country, InvoiceNo, sum(Quantity) as Tot_Quant, sum( Quantity * UnitPrice) as InvoiceValue from orderde group by Country, InvoiceNo").show()
       
   
   //// Window aggregations
   
    val windd = spark.read
   .format("csv")
   .option("inferschema",true)
   .option("header",true)
   .option("path","C:/Users/Pramanik/Documents/Projects/spark/windowdata-201021-002706.csv")
   .load()
   
   val mywind = Window.partitionBy("country")
   .orderBy("weeknum")
   .rowsBetween(Window.unboundedPreceding, Window.currentRow)  // Window.unboundedPreceding or -3/-5 for last 3 days/ 5days
   
   val aggdf = windd.withColumn("Running_Tot",sum("Invoicevalue").over(mywind))
   aggdf.show()
   
   spark.stop
   
 
}