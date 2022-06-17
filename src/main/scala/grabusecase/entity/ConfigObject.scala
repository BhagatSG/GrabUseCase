package grabusecase.entity

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext

import scala.beans.BeanProperty

/**
  * Created by bhagat on 3/21/19.
  */
class ConfigObject(@BeanProperty var sparkConf:SparkConf,
                    @BeanProperty var spark:SparkSession) extends Serializable

