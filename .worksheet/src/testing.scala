object testing {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(60); 
  println("Welcome to the Scala worksheet")
  
  import scala.io.Source;$skip(80); 
   
 
   var moviedata:Map[String, String]  = Map();System.out.println("""moviedata  : Map[String,String] = """ + $show(moviedata ));$skip(161); 
   
   // create a local variable not an rdd
   
   val movi = Source.fromFile("C:/Users/Pramanik/Documents/Projects/spark/movies-201019-002101.dat").getLines();System.out.println("""movi  : Iterator[String] = """ + $show(movi ));$skip(133); 
   
   for( line <- movi) {
   //println(line.split("::")(0))
    moviedata = moviedata + (line.split("::")(0)->line.split("::")(1))
   };$skip(31); 
   
   println(moviedata)}
  
}
