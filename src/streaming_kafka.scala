import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.PreferConsistent
import org.apache.spark.streaming.kafka010.Subscribe
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.LocationStrategies



object streaming_kafka extends App {
  
  def main(args: Array[String]) {
      val  broker_id = "localhost:9092"
      val  groupid = "GRP1"
      val  topics = "testtopic"
      
     val topicset = topics.split(",").toSet
      
      val kafkaParams = Map[String,Object] (
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG  -> broker_id,
      ConsumerConfig.GROUP_ID_CONFIG ->groupid,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringSerializer]
      
      )
      
      val sparkConf = new SparkConf()
       sparkConf.set("spark.app.name"," basic app")
       sparkConf.set("spark.master","local[2]")
  
      val ssc =  new StreamingContext(sparkConf,Seconds(5))
      val sc = ssc.sparkContext

      val mess = KafkaUtils.createDirectStream[String,String](
          ssc, 
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Subscribe[String,String](topicset,kafkaParams)
          
      )
      
      
      val lines = mess.map(_.value()).flatMap(x=>x.split(" "))
          
      val countl = lines.map(x=>(x,1)).reduceByKey(_+_)
     
      countl.print()
      
      ssc.awaitTermination()
   }
}