package grabusecase.utils

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.sql._

import scala.collection.Map

/**
  * Created by bhagat on 3/21/19.
  */
object Utilities {
  val log = Logger.getLogger(getClass.getName)

  val BASE_32 = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z')

  val PRECISION = 6
  val BITS = Array(16, 8, 4, 2, 1)

  def generateGeohash(latitude: String, longitude: String): String = {
    try {
      var latInterval0 = -90.0
      var latInterval1 = 90.0
      var lngInterval0 = -180.0
      var lngInterval1 = 180.0

      val geohash = new StringBuilder()
      var isEven = true

      var bit = 0
      var ch = 0

      while (geohash.length < PRECISION) {
        var mid = 0.0
        if (isEven) {
          mid = (lngInterval0 + lngInterval1) / 2D
          if (longitude.toDouble > mid) {
            ch |= BITS(bit)
            lngInterval0 = mid
          } else {
            lngInterval1 = mid
          }
        } else {
          mid = (latInterval0 + latInterval1) / 2D
          if (latitude.toDouble > mid) {
            ch |= BITS(bit)
            latInterval0 = mid
          } else {
            latInterval1 = mid
          }
        }

        isEven = !isEven;

        if (bit < 4) {
          bit = bit + 1
        } else {
          geohash.append(BASE_32(ch))
          bit = 0
          ch = 0
        }
      }
      geohash.toString()
    } catch {
      case exception: Exception => {
        return null
      }
    }
  }

  def getHourInterval(timeStr: String): String = {
    val time = timeStr.split(" ")(1)
    val start = time.split(":")(0).toInt

    start.toString + "-" + (start + 1).toString
  }

  def getMinInterval(timeStr: String): String = {
    val time = timeStr.split(" ")(1)
    val start = time.split(":")(1).toInt

    val min_start = (start / 10) * 10
    min_start.toString + "-" + (min_start + 10).toString
  }

  def tripTimeDifference(st_tm: String, end_tm: String): String = {
    try {
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

      val st_tm_val = format.parse(st_tm)
      val end_tm_val = format.parse(end_tm)

      val diffMinutes = (end_tm_val.getTime() - st_tm_val.getTime()) / (60 * 1000)
      diffMinutes.toString
    } catch {
      case exception: Exception => {
        return null
      }
    }
  }

  def storeDataFrame(transformedDF: DataFrame, saveMode: String, storageFormat: String, targetTable: String): Boolean = {
    var loadStatus = false
    try {
      val tblCount = transformedDF.count
      log.info("Writing to HIVE TABLE : " + targetTable + " :: DataFrame Count :" + tblCount)
      if (tblCount > 0 && transformedDF != null && saveMode.trim().length() != 0 && storageFormat.trim().length() != 0 && targetTable.trim().length() != 0) {
        log.info("SAVE MODE::" + saveMode + " Table Name :: " + targetTable + " Format :: " + storageFormat)
        transformedDF.write.mode(saveMode).format(storageFormat.trim()).insertInto(targetTable.trim())
        return true
      } else {
        return false
      }

    } catch {
      case e: Exception => e.printStackTrace(); log.info("ERROR Writing to HIVE TABLE : " + targetTable); return false;
    }
    return loadStatus
  }


  def getCurrentTimestamp(): String = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    return dateFormat.format(System.currentTimeMillis())
  }

  def getKafkaProperties(): Map[String, Object] = {
    val prop = new Properties
    val input = Utilities.getClass().getClassLoader().getResourceAsStream("configuration/kafkaConfig.properties");
    prop.load(input)

    val configuration: Map[String, Object] = Map[String, Object](
      //"metadata.broker.list" -> prop.getProperty("metadata.broker.list"),
      "bootstrap.servers" -> prop.getProperty("bootstrap.servers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> prop.getProperty("group.id"),
      "auto.offset.reset" -> prop.getProperty("auto.offset.reset"),
      "enable.auto.commit" -> (prop.getProperty("enable.auto.commit").toBoolean: java.lang.Boolean),
      "fetch.message.max.bytes" -> "15000000",
      "max.partition.fetch.bytes" -> "15000000")


    return configuration
  }

}
