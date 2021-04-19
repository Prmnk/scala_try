import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext


object basic_parallelize extends App {
  
 // Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[1]","parallel")
  
  val list = List(
                "WARN: Tuesday 4 August 0605",    
                "ERROR: Tuesday 4 August 0605",  
                "WARN: Tuesday 4 August 0405",  
                "ERROR: Tuesday 4 August 0607",  
                "WARN: Tuesday 4 August 0605",  
                "ERROR: Tuesday 4 August 0606",  
                "WARN: Tuesday 4 August 0605"
  )
  
  
   val rdd1 = sc.parallelize(list)
   
   val rdd2 = rdd1.map(x=> {
     val cols = x.split(":")
     val loglevel = cols(0)
     (loglevel,1)    
     
   })
   
   val rdd3 = rdd2.reduceByKey(_+_)
   
   rdd3.collect().foreach(println)
  
}

 

