import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

object Dataframe_MergeDims extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf
  
  sparkConf.set("spark.app.name","explicit_schema").set("spark.master","local[*]")
  
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  
  
  val entity = spark.read
  .format("csv")
  .option("header",true)
  .option("inferschema",false) // true if schema not defined
  .option("mode", "PERMISSIVE") // default value
  .option("path","C:/Users/Pramanik/Desktop/files_dbk/entity.csv")
  .load()
  
  val entity_delta = spark.read
  .format("csv")
  .option("header", true)
  .option("inferschema", false) // true if schema not defined
  .option("mode", "PERMISSIVE")
  .option("path","C:/Users/Pramanik/Desktop/files_dbk/entity_delta.csv")
  .load()
  
  //import spark.implicits._
  
  val entity_1 = entity.withColumn("current_timestamp",current_timestamp().as("current_timestamp"))
   
  val entity_delta_1 = entity_delta.withColumn("current_timestamp",current_timestamp().as("current_timestamp"))
  println(entity_1.count())
  println(entity_delta_1.count())
  
  entity_1.createOrReplaceTempView("entity")
  entity_delta_1.createOrReplaceTempView("delta")
  
  // Merge dimension -- using join for updates, left join for existing records, right join for inserting new records
  val entity_final = spark.sql("""Select delta.* from entity join delta on entity.Entity_ID = delta.Entity_ID 
               union 
               Select delta.* from entity right join delta on entity.Entity_ID = delta.Entity_ID where entity.Entity_ID is NULL
               union
               Select entity.* from entity left join delta on entity.Entity_ID = delta.Entity_ID where delta.Entity_ID is NULL
                 """)
  
  println(entity_final.count())
  entity_final.show()
  
  
   entity_final
   .repartition(1)
   .write 
   .format("csv")
   .mode(SaveMode.Overwrite)  
   .option("path","C:/Users/Pramanik/Desktop/files_dbk/warehouse")   
   .save()
   
   
   
   
   
   
   
  spark.stop()
}