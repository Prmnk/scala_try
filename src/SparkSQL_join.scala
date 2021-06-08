import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._


object SparkSQL_join extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf
  sparkConf.set("spark.app.name"," spark_sql_join_app")
  sparkConf.set("spark.master","local[2]")
  
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  
  
  val df1 = spark.read
  .format("csv")
  .option("header",true)
  .option("path","C:/Users/Pramanik/Documents/Projects/spark/files_test/tab2.csv")
  .load()
  
  val df1_exp = df1.withColumn("region_exploded",explode(split(col("regions"),",")))
  
  val df2 = spark.read
  .format("csv")
  .option("header",true)
  .option("path","C:/Users/Pramanik/Documents/Projects/spark/files_test/tabb3.csv")
  .load()
  
  df1_exp.createOrReplaceTempView("item")
  df2.createOrReplaceTempView("dat")
  
  spark.sql("""Select distinct item, manufacturing_plant, region_exploded , max_delivery_date 
    from (Select  item, manufacturing_plant, region_exploded , max_delivery_date, rank()over(partition by item order by max_delivery_date desc) as rk 
    from item join dat on item.region_exploded = dat.region ) k where rk = 1 """).show()
  
  spark.stop
  
  
 
}