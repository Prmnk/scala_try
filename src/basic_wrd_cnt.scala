import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object basic_wrd_cnt extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
   val sc = new SparkContext("local[*]","basic_wrd_cnt")

   val rdd1 = sc.textFile("C:/Users/Pramanik/Documents/Projects/spark/inp/search_data-201008-180523.txt")
   
   val rdd2 = rdd1.flatMap(x=>x.split(" ")).map(x=>x.toLowerCase())
   
   //val rdd3 = rdd2.flatMap(x=>x.split(","))
   
   val rdd4 = rdd2.filter(x=> x.indexOf("deta")>=0|| x.indexOf("data")>=0)
       
   val rdd5 = rdd4.map(x=>(x,1))
   
   val rdd6 = rdd5.reduceByKey((x,y)=>x+y).sortBy(x=>x._2,false)
   
   for (res<- rdd6.collect()){
     val word = res._1
     val cnt = res._2
     println(s"$word:$cnt")
     
   }
   
   
   //rdd6.saveAsTextFile("C:/Users/Pramanik/Documents/Projects/spark/inp/search_data_out1")
   
   scala.io.StdIn.readLine()

}