


  

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode


object Dataframe_delta_processing_inc extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf
  
  sparkConf.set("spark.app.name","fact_pivot_del").set("spark.master","local[2]").set("spark.sql.sources.partitionOverwriteMode", "dynamic")
 
  
  val spark = SparkSession.builder.config(sparkConf).getOrCreate()
  
  val attributeschema = StructType(List( // with spark datatypes
      StructField("attribute_id",IntegerType),
      StructField("attribute_name",StringType),
      StructField("attribute_display",StringType),
      StructField("a",StringType),
      StructField("b",StringType),
      StructField("tenant_id",StringType)
  ))
  
  val attribute = spark.read
  .format("csv")
  .option("header",false)
  .option("inferschema",false) // true if schema not defined
  .schema(attributeschema)
  .option("mode", "PERMISSIVE") // default value
  .option("path","C:/Users/Pramanik/Desktop/files_dbk/attribute.csv")
  .load()
  
  val entity = spark.read
  .format("csv")
  .option("header",true)
  .option("inferschema",false) // true if schema not defined
  .option("mode", "PERMISSIVE") // default value
  .option("path","C:/Users/Pramanik/Desktop/files_dbk/entity.csv")
  .load()
  
  
  val entityattribute = spark.read
  .format("csv")
  .option("header",true)
  .option("inferschema",false) // true if schema not defined
  .option("mode", "PERMISSIVE") // default value
  .option("path","C:/Users/Pramanik/Desktop/files_dbk/entity_attribute_delta.csv")
  .load()
  .withColumnRenamed("attribute_id","attribute_id_a")
  .withColumnRenamed("entity_id","entity_id_a")
  
  //attribute.show()
  entity.show()
  entityattribute.show()
  
  val joincondition1 = entityattribute.col("attribute_id_a") === attribute.col("attribute_id")
  val jointype1 = "inner"
  
  val df_entityattribute = entityattribute.join(broadcast(attribute),joincondition1,jointype1)
  
  val df_entityattribute_a = df_entityattribute
  .select(column("entity_id_a"),col("attribute_name"),col("attribute_val"))
  .dropDuplicates()
  
  
  val joincondition2 = df_entityattribute_a.col("entity_id_a") === entity.col("Entity_ID")
  val jointype2 = "inner"
  val df_entityattribute_final = df_entityattribute_a.join(broadcast(entity),joincondition2,jointype2)
  
  var parts = df_entityattribute_final
  .select(col("attribute_name")).distinct()
  .collect().map(_(0)).toList
  
  
  var finalh  = "C:/Users/Pramanik/Desktop/files_dbk/warehouse/aggregate/attribute_name={" 
  parts.foreach(finalh += _+",")
      
  val lik = finalh.substring(0, finalh.length()-1)+"}/*"
  println(lik)
  val entityattribute_old = spark.read
  .option("header",true)
  .option("inferschema",false) // true if schema not defined
  .option("mode", "PERMISSIVE") // default value
  .csv(lik)
  .withColumnRenamed("attribute_id","attribute_id_c")
  .withColumnRenamed("entity_id","entity_id_c")
  
  
  entityattribute_old.show()
  
  //val cols = List("ExcaliburWellID","WorkingInterest","SHLatitude")
  
  //val df =  df_entityattribute_final.where()
  
  
   df_entityattribute_final.repartition(1)
   .write 
   .format("csv")
   .partitionBy("attribute_name")
   .mode(SaveMode.Overwrite)  
   .option("path","C:/Users/Pramanik/Desktop/files_dbk/warehouse/aggregate")   
   .save()
  
  spark.stop()
  
}