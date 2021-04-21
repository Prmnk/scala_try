import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import scala.io.Source


object basic_movierating extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","movie")
  
  
  // try broacasting got faster joins
  
  def moviedata():Map[String, String] = {
   var moviedata:Map[String, String]  = Map()
   
   // create a local variable not an rdd
   
   val movi = Source.fromFile("C:/Users/Pramanik/Documents/Projects/spark/movies-201019-002101.dat").getLines()
   
   for( line <- movi) {
   //println(line.split("::")(0))
    moviedata = moviedata + (line.split("::")(0)->line.split("::")(1))
   }
   
  moviedata
 }
  
  val movo = sc.textFile("C:/Users/Pramanik/Documents/Projects/spark/movies-201019-002101.dat")
  
   val mov1 = movo.map(x=>(x.split("::")(0),x.split("::")(1)))
  
  // another cool way to create a map from RDD to broadcast
  //val brdcstmov = sc.broadcast(mov.collect.toMap)
  
  var mov = sc.broadcast(mov1.collect.toMap)//sc.broadcast(moviedata)
  
  val rat = sc.textFile("C:/Users/Pramanik/Documents/Projects/spark/ratings-201019-002101.dat")
  
  
  
  //mov.take(5).foreach(println)
  
  //rat.take(5).foreach(println)
  
  val rat1 = rat.map(x=>(x.split("::")(1),x.split("::")(2))).mapValues(x=>(x.toFloat,1.0)).reduceByKey((x,y)=>(x._1+y._1, x._2+y._2))
      
  val rat2 = rat1.filter(x=>x._2._2>100)
  
  val rat3 = rat2.mapValues(x=> x._1/x._2).filter(x=>x._2>4.5)
  
 
  
  val lookuprdd = rat3.map(x=>{
    val k = x._1
    val v1 = x._2
    val mname = mov.value.get(x._1).get
    (mname,v1)
  })
 

  //val joinedrdd = lookuprdd.map(x=> (x._2._1, x._2._2))
      
  //val joinedrdd = mov1.join(rat3).map(x=> (x._2._1, x._2._2))
    
  for (res<- lookuprdd.collect()){
     val movie = res._1
     val rating = res._2
     println(s"$movie:$rating")}
  
  
}