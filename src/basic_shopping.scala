import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext


object basic_shopping extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","shopping")
  
  val rdd1  = sc.textFile("C:/Users/Pramanik/Documents/Projects/spark/customer-orders.csv")
  
 //  rdd1.top(5).foreach(println)
  
  val rdd2 = rdd1.map(x=> (x.split(",")(0),x.split(",")(2).toFloat))
      
      
  val rdd3 = rdd2.reduceByKey(_+_)
  
  val rdd4 = rdd3.sortBy(x=>x._2, false)
  
  val rdd5 = rdd4.collect().take(10)
  
  rdd5.foreach(println)
   
 // rdd5.take(10).foreach(println)
  
  
}