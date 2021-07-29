import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode


object Dataframe_json_processing extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","json_process")
  sparkConf.set("spark.master","local[2]")
  
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  
  //val sc = new SparkContext(sparkConf)
  /*
  val inputjson = spark.read
  .format("json")
  .option("multiLine", true)
  .option("mode", "PERMISSIVE")
  .option("path", "C:/Users/Pramanik/Documents/Projects/test.json")
  .load()
  */
  //val inputjson  = spark.read.option("multiline", "true").json("C:/Users/Pramanik/Documents/Projects/Agri/Mandi_20_07_2021_11_56_20.json")
  
  //val jsonRDD = sc.wholeTextFiles("C:/Users/Pramanik/Documents/Projects/Agri/Mandi_20_07_2021_11_56_20.json").map(x => x._2)
  
  val inputjson = spark.read
  .json(spark.sparkContext.wholeTextFiles( "C:/Users/Pramanik/Documents/Projects/test.json").values)
  
  inputjson.createOrReplaceTempView("data")
  
  val commodity = spark.sql("Select distinct commodity,variety from data")
  
  val area = spark.sql("Select distinct state, district, market from data")
  
  val price_fact = spark.sql("Select replace(arrival_date, '\\/','-') as arrival_date, state,district,market,max_price,min_price,modal_price,variety, commodity from data")

  price_fact.show()
  
  price_fact.repartition(1)
   .write 
   .format("csv")
   .mode(SaveMode.Append)  
   .option("path","C:/Users/Pramanik/Documents/Projects/warehouse/price_fact")   
   .save()
   
   area.repartition(1)
   .write 
   .format("csv")
   .mode(SaveMode.Append)  
   .option("path","C:/Users/Pramanik/Documents/Projects/warehouse/area")   
   .save()
   
   commodity.repartition(1)
   .write 
   .format("csv")
   .mode(SaveMode.Append)  
   .option("path","C:/Users/Pramanik/Documents/Projects/warehouse/commodity")   
   .save()
   
  spark.stop
}