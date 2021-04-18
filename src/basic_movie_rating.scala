import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext


object basic_movie_rating extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","ratings")
  
  val rdd1 = sc.textFile("C:/Users/Pramanik/Documents/Projects/spark/ratings-201019-002101.dat")
  
 // rdd1.top(5).foreach(println)
  
  val rdd2 = rdd1.map(x=>(x.split("::")(2).toInt))
  
  val rdd3 = rdd2.countByValue
  
  rdd3.take(5).foreach(println)
 
  
}