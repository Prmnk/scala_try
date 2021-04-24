import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.sql.Timestamp

case class OrdersData  (order_id : Int,
                        order_date : Timestamp,
                        order_customer_id : Int,
                        order_status : String)

object Dataframe_dataset extends App {
  
  val sparkConf = new SparkConf()
  
  sparkConf.set("spark.app.name","dataset_df").set("spark.master","local[2]")
  
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  
  //Dataframe
  val ordersdf :Dataset[Row] = spark.read // Dataset[Row] = data frame --> type is resolved at compile time
  .option("header",true)
  .option("inferschema",true)
  .csv("C:/Users/Pramanik/Documents/Projects/spark/orders-201019-002101.csv")
  
  ordersdf.filter("order > 10000") // Errors out at complied time because no column name "order" exists
  
  
  import spark.implicits._ // needed to convert dataframe <> dataset , should be created after creating session
  
  // Dataset
  val ordersdat  = ordersdf.as[OrdersData]  // Dataset[class] = Dataset , now ordersdat is dataset
  
  ordersdat.filter(x=> x.order_id < 10) // order instead of order_id gives error here itself
  
  
  
  spark.stop()
}