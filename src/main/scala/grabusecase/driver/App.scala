package grabusecase.driver

import grabusecase.utils.{AWSkeyPropertiesReader, SparkContextFactory}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * Hello world!
 *
 */
object App {
  // def void Test(drivername : String) ={


  def main(args: Array[String]): Unit ={

    println("Hello World " + this.getClass.getName)
    println("Hello World YEs " + this.getClass.getSimpleName)
    //Test demandSupplyRatioCalculator(this.getClass.getSimpleName)

    /*val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    println("Hello World 2");
    val topics = Array("topicA", "topicB")
    val BUCKET_NAME = "grab-batch-data"

    val spark = SparkContextFactory getSparkContext("App")
    val sqlContext = spark.sqlContext

    val ssc = new StreamingContext(spark.sparkContext,Seconds(5))
    val msges = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )
    //val msges = KafkaUtils.createDirectStream(ssc,PreferConsistent,Subsc)


    msges.map(record => (record.key, record.value))
    //KafkaUtils.createStream(ssc, zkQuorum, groupId, topics)
    //createDirectStream[String, String](configObject.getSsc(), PreferConsistent, Subscribe[String, String](propertiesObject.getTopicList().split(','), kafkaParams))
    //val configurationObject:ConfigObject = SetUpConfiguration.setup(10)

    //val inputDF = configurationObject.getSpark().sqlContext.read.format("csv").option("header","true").load("s3n://rampradhanbucket/InputFiles/DriverLocationCSV.csv")

    println("Hello World 3");
    val inputRDD = spark.sparkContext.textFile("s3n://grab-extract-data/DriverLocationCSV.csv")

    println("Hello World 4");
    inputRDD.take(20).foreach(println)


    println("Hello World 5");
    val inputDF = sqlContext.read.format("csv").option("header","true").load("s3n://grab-extract-data/DriverLocationCSV.csv")

    println("Hello World 6");
    //inputDF.show(15,false)

    println("End of Program CSV testing Successful")*/
  }
}
