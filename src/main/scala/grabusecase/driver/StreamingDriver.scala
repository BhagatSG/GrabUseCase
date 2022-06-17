package grabusecase.driver

import java.math.BigInteger

import grabusecase.config.SetUpConfiguration
import grabusecase.entity.ConfigObject
import grabusecase.process.HistoryLoad
import grabusecase.utils.Utilities
import org.apache.log4j.Logger
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.Map

/**
  * Created by bhagat on 3/21/19.
  */
object StreamingDriver {
  val log = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val applicationName = "Grab-Usecase-Streaming-Driver"

    if (args == null || args.isEmpty) {
      println("Invalid number of arguments passed.")
      println("Arguments Usage: <Waiting Window in seconds> <Kafka Topic List> <Optional arg: Properties file path>")
      println("Stopping the flow")
      System.exit(1)
    }

    log.info("Arguments Assignment started #####################################")
    val pollingWindow = new BigInteger(String.valueOf(args(0).trim())).longValue()
    val topicArr = String.valueOf(args(1).trim()).split("#")
    val tblTopicMapping = String.valueOf(args(2).trim())
    log.info("polling window "+pollingWindow+" args 0 "+args(0))
    log.info("topicArr "+args(1))
    log.info("tblTopicMapping "+tblTopicMapping+" args 2 "+args(2))

    log.info("Spark Streaming Context set up #####################################")
    val configObject: ConfigObject = SetUpConfiguration.setup(applicationName)
    val streamingContext = new StreamingContext(configObject.spark.sparkContext,Seconds(pollingWindow))

    val kafkaParams: Map[String, Object] = Utilities.getKafkaProperties()

    println("HistoryLoad DataPipeline call")
    log.info("History Load DataPipeLine Started #####################################")
    var status = HistoryLoad.dataPipeLine(kafkaParams, configObject, topicArr, tblTopicMapping, streamingContext)

    println("HistoryLoad DataPipeline completed")

    if(status){
      streamingContext.start()
      log.info("Triggered the Kafka Batch Successfully")
      streamingContext.awaitTermination()
    }
    else{
      log.error("Error triggering the Kafka Batch.")
      streamingContext.stop(true, true)
    }

  }
}
