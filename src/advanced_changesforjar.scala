import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext




object advanced_changesforjar  { // extends app is removed

  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def main(args:Array[String]) {
  
  val sc = new SparkContext() // removed loca[*] to make sure it runs on cluster
  
  val rdd1 = sc.textFile(args(0)) /// to parse the file path dynamically
  
  val rdd2 = rdd1.map(x=>(x.split(",")(0),x.split(",")(1)))
  
  rdd2.groupByKey().collect().foreach(x=> println(x._1,x._2.size))
  
  }
  scala.io.StdIn.readLine()
}
