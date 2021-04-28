import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._


object SparkSql_DF_DS extends App {
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name"," spark_sql)app")
  sparkConf.set("spark.master","local[2]")
  
  val spark = SparkSession.builder().config(sparkConf).enableHiveSupport.getOrCreate()
  
  case class Person(name: String, age : Int, city: String)
  
  def agecheck(age:Int):String = {
    if (age >18) "Y" else "N"
    
  }
  
  val df = spark.read
  .option("header",true) 
  .option("inferSchema",true) 
  .csv("C:/Users/Pramanik/Documents/Projects/spark/-201025-223502.dataset1") 
  
  val df1 : Dataset[Row] = df.toDF("name","age","city") // dataframe with header columns
  
  import spark.implicits._
  
  val ds1 = df1.as[Person]  // dataframe to dataset - to avoid compile time errors
  
  val ds3 = ds1.groupByKey(x=> x.city)
  
  val df2 = ds1.toDF()  // dataset to dataframe
  
  val newfunc = udf(agecheck(_:Int):String) // register the function as udf --> does not add it to catalog
  
  spark.catalog.listFunctions().filter(x=> x.name =="newfunc").show()
  
  spark.udf.register("newfunc2",agecheck(_:Int):String) // another way to register udf
  
  spark.udf.register("newfunc3",(x:Int)=>{if (x>18) "y" else "N"}) // anonymous function
  
  spark.catalog.listFunctions().filter(x=> x.name =="newfunc2").show()
  
  val df3 = df1.withColumn("adult", newfunc(col("age"))) // for a new column addition
  
  val df4 = df3.withColumn("adult1", expr("newfunc2(age)"))
  
  df4.createOrReplaceTempView("order") // use sql logic for new column or call the udf in sql syntax
  
  val df5 = spark.sql("Select name, age, city, adult, adult1, case when age > 18 then 'Y' else 'N' end as adult2, newfunc2(age) as adult3 from order")
  
  df3.show()
  
  df4.show()
  
  df5.show()
  
  spark.stop
  
}