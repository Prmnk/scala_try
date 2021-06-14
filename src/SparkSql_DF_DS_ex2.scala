import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DateType
import java.sql.Date
import org.apache.spark.sql.functions._
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import java.text.SimpleDateFormat
import org.apache.spark.sql.SaveMode


object SparkSql_DF_DS_ex2 extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR) // Only Errors to be shown on console window
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","DF_DS_Ex2")
  sparkConf.set("spark.master","local[2]")
  
  val spark = SparkSession.builder().config(sparkConf).enableHiveSupport.getOrCreate()
  
  // Define explicit schema for the files getting imported
  
  val delivery_schema = StructType(List(
      StructField("item",StringType),
      StructField("manufacturing_plant",StringType),
      StructField("delivery_centers",StringType),
      StructField("regions",StringType)
      )      
  )
  
  val date_schema = StructType(List(
      StructField("region",StringType),
      StructField("max_delivery_date",DateType)
  ))
  
  
  // Define Case class to convert dataframe to dataset later to avoid compile time errors
  
  case class delivery(item:String, manufacturing_plant:String, delivery_centers:String, regions:String, regions_exploded: String)
  
  case class date_(region:String, max_delivery_date:Date)
  
  
  // Import data
  
  val df_delivery = spark.read
  .format("csv")
  .option("header",true)
  .schema(delivery_schema)
  .option("inferschema",false) // true if schema not defined
  .option("mode", "PERMISSIVE") // default value
  .option("path","C:/Users/Pramanik/Documents/Projects/spark/files_test/tab2.csv")
  .load()
  
  val df_date = spark.read
  .format("csv")
  .option("header", true)
  .schema(date_schema)
  .option("inferschema", false) // true if schema not defined
  .option("mode", "PERMISSIVE")
  .option("path","C:/Users/Pramanik/Documents/Projects/spark/files_test/tabb3.csv")
  .load()
 
  // create and register a UDF 
  
  def get_year(dates:Date):String = {
    val dateFormat = new SimpleDateFormat("YYYY")
    val year = dateFormat.format(dates)
    return year
    
  }
 
  spark.udf.register("get_year_udf",get_year(_:Date):String)
  
  // Regions column in df_delivery dataframe is a comma separated value that needs to be exploded for join 
  
  val df_delivery_exp = df_delivery.withColumn("regions_exploded", explode(split(col("regions"),",")))
  
  // drop all rows with any column as NULL
  val df_delivery_exp_clean =  df_delivery_exp.na.drop("any") 
  
  // define join conditions
  val join_conditions = df_delivery_exp_clean.col("regions_exploded")===df_date.col("region")
  val join_type = "left"
 
  val joined_df = df_delivery_exp_clean.join(broadcast(df_date), join_conditions, join_type)
  
  import spark.implicits._
 
  
  // rank over delivery dates and pick the record with latest date, also drop duplicates because of the join , create a new column
  
   val windo = Window.partitionBy("item", "manufacturing_plant").orderBy("max_delivery_date")
  
   
   val resultdf = joined_df
  .select(column("item"),col("manufacturing_plant"), $"delivery_centers",'regions,column("max_delivery_date"))
  .withColumn("Ranked",rank().over(windo)) // example of window function
  .withColumn("Date_plus_2",date_add(col("max_delivery_date"),2)) // example of new column with manipulated data
  .withColumn("testing",expr("concat(item ,'-',manufacturing_plant)")) // example for expr
  .withColumn("Year",expr("get_year_udf(max_delivery_date)") )
  .filter("Ranked = 1")
  .dropDuplicates()
 
  
  // Another way using Spark SQL, create 2 views and run a simple SQL statement with join and rank function
  
  
  df_delivery_exp.createOrReplaceTempView("item")
  df_date.createOrReplaceTempView("dat")
  
  // UDF has been used inside the sql statement below
  
  spark.sql("""Select distinct item, manufacturing_plant, regions_exploded , max_delivery_date ,get_year_udf(max_delivery_date) as d
    from (Select  item, manufacturing_plant, regions_exploded , max_delivery_date, rank()over(partition by item order by max_delivery_date desc) as rk 
    from item join dat on item.regions_exploded = dat.region ) k where rk = 1 """).show()
  
    
     
  // convert to dataset using case class - if we need to use dataset api
  
  val ds_del = df_delivery_exp.as[delivery]
  val ds_date = df_date.as[date_]
  
  
  // write results to data warehouse with partitions
  
  resultdf.write
  .format("csv")
  .mode(SaveMode.Overwrite) 
  .option("path","C:/Users/Pramanik/Documents/Projects/spark/output_par_db1")   
  .partitionBy("item")
  .saveAsTable("order_db.item_tbl")
   
  
  spark.stop
  
  
}