package grabusecase.process

import java.net.ConnectException
import java.util.HashMap

import grabusecase.entity.ConfigObject
import grabusecase.utils.Utilities
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}

import scala.collection._

/**
  * Created by bhagat on 3/22/19.
  */
object HistoryLoad {
  val logger = Logger.getLogger(getClass.getName)

  def dataPipeLine(kafkaParams: Map[String, Object], configObject: ConfigObject, topicArr: Array[String], tblTopicMapping: String, streamingContext: StreamingContext): Boolean = {
    val messages = KafkaUtils.createDirectStream[String, String](streamingContext, LocationStrategies.PreferConsistent, Subscribe[String, String](topicArr, kafkaParams))
    val topicList = topicArr.toList

    import scala.collection.JavaConversions._
    val tblMappingArr = tblTopicMapping.split("#")
    logger.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@Mapping Array: " + tblMappingArr.foreach {
      println
    })
    val tblTopicMap: Map[String, String] = new HashMap[String, String]()
    for (element <- tblMappingArr) {
      logger.info("****************************" + element)
      val topic = element.split("\\|")(0)
      val histTbl = element.split("\\|")(1)
      tblTopicMap.put(topic, histTbl)
    }

    try {
      messages.foreachRDD(rdd => {
        if (!rdd.isEmpty()) {
          println("RDD is not empty")
          val eff_frm_date = Utilities.getCurrentTimestamp()
          //val spark=configObject.getSpark()
          val kafkaRDD = rdd.map(x => Row(x.value().toString(), x.offset(), x.topic()))
          val hist_tbl_schema = (new StructType).add("payload", StringType).add("offset", LongType).add("topic", StringType)
          val input_df_single_col = configObject.spark.sqlContext.createDataFrame(kafkaRDD, hist_tbl_schema)
          input_df_single_col.createOrReplaceTempView("hist_temp")

          for (topic <- topicList) {
            println("Topic call " + topic)
            val histTbl = "grabusecase." + tblTopicMap(topic)
            logger.info("############ Extracting data for Topic : " + topic + " and dumping to table : " + histTbl)
            val histDf = configObject.spark.sqlContext.sql(f"""select payload,cast(offset as String) as offset,cast("$eff_frm_date" as timestamp) as loadtime from hist_temp where topic='$topic'""")
            val loadStatus = Utilities.storeDataFrame(histDf, "Append", "ORC", histTbl)

            if (loadStatus) {
              logger.info("History Table Load Completed. Now Starting transformations")
              val payloadDf = histDf.drop(histDf("offset")).drop(histDf("loadtime"))

              if (topic == "congestionEstimator") {
                val dataDf = payloadDf.select(get_json_object(payloadDf("payload"), "$.VendorID")
                  .alias("trip_id"), get_json_object(payloadDf("payload"), "$.lpep_pickup_datetime")
                  .alias("pickup_datetime"), get_json_object(payloadDf("payload"), "$.Lpep_dropoff_datetime")
                  .alias("dropoff_datetime"), get_json_object(payloadDf("payload"), "$.Pickup_longitude")
                  .alias("pickup_longitude"), get_json_object(payloadDf("payload"), "$.Pickup_latitude")
                  .alias("pickup_latitude"), get_json_object(payloadDf("payload"), "$.Dropoff_longitude")
                  .alias("dropoff_longitude"), get_json_object(payloadDf("payload"), "$.Dropoff_latitude")
                  .alias("dropoff_latitude"), get_json_object(payloadDf("payload"), "$.Trip_distance").alias("trip_distance"))

                println("Data save for congestionEstimator")
                dataDf.coalesce(1).write.format("csv").option("header", "true").mode("Overwrite")
                  .save("s3n://grab-realtime-data/TrafficEstimator/")

              } else if (topic == "customerData") {
                val custDemandDf = payloadDf.select(get_json_object(payloadDf("payload"), "$.cust_id")
                  .alias("cust_id"), get_json_object(payloadDf("payload"), "$.cust_latitude")
                  .alias("cust_latitude"), get_json_object(payloadDf("payload"), "$.cust_longitude")
                  .alias("cust_longitude"), get_json_object(payloadDf("payload"), "$.cust_datetime")
                  .alias("cust_datetime"))

                println("Data save for customerData")
                custDemandDf.coalesce(1).write.format("csv").option("header", "true").mode("Overwrite")
                  .save("s3n://grab-realtime-data/DemandData/")

              } else if (topic == "driverData") {
                val cabSupplyDf = payloadDf.select(get_json_object(payloadDf("payload"), "$.cab_id")
                  .alias("cab_id"), get_json_object(payloadDf("payload"), "$.cab_latitude")
                  .alias("cab_latitude"), get_json_object(payloadDf("payload"), "$.cab_longitude")
                  .alias("cab_longitude"), get_json_object(payloadDf("payload"), "$.cab_datetime")
                  .alias("cab_datetime"))

                println("cabSupplyDf write to S3  Start for topic: "+topic)
                cabSupplyDf.coalesce(1).write.format("csv").option("header", "true").mode("Overwrite")
                  .save("s3n://grab-realtime-data/SupplyData/")
              }
            }
            else {
              logger.error("Failed to load History Table. Stopping the Flow!")
            }
          }
        }
      })
    }
    catch {
      case exception: Exception => {
        logger.error(exception.printStackTrace())
        return false
      }
      case nseException: NoSuchElementException => {
        logger.error("No Such element found: " + nseException.printStackTrace())
        return false
      }
      case anaException: AnalysisException => {
        logger.error("SQL Analysis Exception: " + anaException.printStackTrace())
        return false
      }
      case connException: ConnectException => {
        logger.error("Connection Exception: " + connException.printStackTrace())
        return false
      }
    }
    return true;
  }
}
