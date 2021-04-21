object testing {
  println("Welcome to the Scala worksheet")
  
  import scala.io.Source
   
 
   var moviedata:Map[String, String]  = Map()
   
   // create a local variable not an rdd
   
   val movi = Source.fromFile("C:/Users/Pramanik/Documents/Projects/spark/movies-201019-002101.dat").getLines()
   
   for( line <- movi) {
   //println(line.split("::")(0))
    moviedata = moviedata + (line.split("::")(0)->line.split("::")(1))
   }
   
   println(moviedata)
  
}