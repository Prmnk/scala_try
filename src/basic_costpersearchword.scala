import scala.io.Source

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext



object basic_costpersearchword extends App {
  
 Logger.getLogger("org").setLevel(Level.ERROR)
 
 val sc = new SparkContext("local[*]","search_cost")
 
 
 def laodstupidwords():Set[String] = {
   var stupidwords:Set[String] = Set()
   
   // create a local variable not an rdd
   
   val stpwords = Source.fromFile("C:/Users/Pramanik/Documents/Projects/spark/boringwords-201014-183159.txt").getLines()
   
   for( line <- stpwords) {
     stupidwords = stupidwords + line     
   }
   
   stupidwords
 }
 
 val rdd1 = sc.textFile("C:/Users/Pramanik/Documents/Projects/spark/bigdatacampaigndata-201014-183159.csv")
 
 //broadcast variable to all worker nodes
 
 var stwrd = sc.broadcast(laodstupidwords)
 
 //rdd1.take(5).foreach(println)
 
 val rdd2 = rdd1.map(x=>(x.split(",")(0), x.split(",")(10).toFloat))
 
 val rdd3 = rdd2.map(x=>(x._2,x._1)).flatMapValues(x=>x.split(" "))
 
 val rdd4 = rdd3.map(x=>(x._2,x._1)).filter(x=> !stwrd.value(x._1)).reduceByKey(_+_).sortBy(x=>x._2, false) // filter out not needed words
 
 rdd4.take(100).foreach(println)
 
 
 
 
 //
}