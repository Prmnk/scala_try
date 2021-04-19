import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext


object basic_groupbykey extends App {
   Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","accumulator")
  
  val rdd1 = sc.textFile("C:/Users/Pramanik/Documents/Projects/spark/biglog-201105-152517.txt")
  
  val rdd2 = rdd1.map(x=>(x.split(",")(0),x.split(",")(1)))
  
  rdd2.groupByKey().collect().foreach(x=> println(x._1,x._2.size))
  
  scala.io.StdIn.readLine()
}