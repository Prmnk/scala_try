import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object basic_num_friends extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  
  
  val sc = new SparkContext("local[*]","friends")
  
  val rdd1 = sc.textFile( "C:/Users/Pramanik/Documents/Projects/spark/fakefriends.csv")
  
  val rdd2 = rdd1.map(x=>(x.split(",")(2),x.split(",")(3).toFloat))
  
  //val rdd4 = rdd2.map(x=>(x._1, (x._2,1)))
  
  val rdd4 = rdd2.mapValues(x=>(x,1))
  
  val rdd5 = rdd4.reduceByKey((x,y)=>(x._1+y._1, x._2+y._2))
  
  val rdd6 = rdd5.map((x=>(x._1, x._2._1/x._2._2))).sortBy(x=>x._2,false)
  
  val rdd7= rdd6.map(x=>(x._1,x._2 - x._2%0.01, if (x._2>200) 'G' else 'N' ))
      
  rdd7.collect.foreach(println)
      
  //val rdd3 = rdd2.reduceByKey((x,y)=>(x+y)/2).sortBy(x=>x._2,false)
  
  //rdd3.collect.foreach(println)
  
  
}
