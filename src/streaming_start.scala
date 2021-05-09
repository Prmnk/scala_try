import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.streaming._



object streaming_start extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[2]","stream1")
  
  val ssc =  new StreamingContext(sc,Seconds(5)) // create streaming context with spark context and time interval for DStream
  
  val lines = ssc.socketTextStream("localhost",9998)  // port on local machine to read the stream from 
  
  val rdd2 = lines.flatMap(x=>x.split(" "))
  
  val rdd3 = rdd2.map(x=>(x,1))
  
  val rdd4 = rdd3.reduceByKey(_+_)
  
  rdd4.print()
  
  ssc.start
}