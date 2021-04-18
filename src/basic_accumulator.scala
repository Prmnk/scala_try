import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext


object basic_accumulator extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","accumulator")
  
  val rdd1 = sc.textFile("C:/Users/Pramanik/Documents/Projects/spark/windowdata-201025-223502.csv")
  
  val rdd2 = rdd1.map(x=>x.split(",")(0))
  
  var accum = sc.longAccumulator("accumul")
  
  rdd2.foreach(x=> if (x.indexOf("India")>=0) accum.add(1))
  
  println(accum)
  
  
  
  
}