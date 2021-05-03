import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object Advanced_salting extends App {
   Logger.getLogger("org").setLevel(Level.ERROR)
   
   def salted (x :String) : Array[String]= {
     
     val start = 1
     val end   = 60
     val rnd = new scala.util.Random
     val num = start + rnd.nextInt( (end - start) + 1 ) 
     var z = new Array[String](2)
     
     z(0) = x.split(",")(0)+num.toString()
     z(1) = x.split(",")(1)
    return z 
   }
  
  val sc = new SparkContext("local[*]","accumulator")
  
  val rdd1 = sc.textFile("C:/Users/Pramanik/Documents/Projects/spark/biglog-201105-152517.txt")
  
  val rdd2 = rdd1.map(x =>(salted(x)(0),salted(x)(1)))
 
  val rdd3 = rdd2.groupByKey()
 
  val rdd4 = rdd3.map(x=> (x._1,x._2.size))
  
  val rdd5 = rdd4.map(x=> { 
    if(x._1.substring(0,4)== "WARN") ("WARN",x._2)
    else if(x._1.substring(0,5)== "FATAL") ("FATAL",x._2) 
    else if(x._1.substring(0,5)== "DEBUG") ("DEBUG",x._2) 
    else  ("INFO",x._2) 
  }
    )
    
   val rdd6 = rdd5.reduceByKey(_+_) 
  
  rdd6.collect.foreach(println)
}