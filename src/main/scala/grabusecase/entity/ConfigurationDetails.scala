package grabusecase.entity

import java.text.SimpleDateFormat

/**
  * Created by bhagat on 3/17/19.
  */
trait ConfigurationDetails {
  val MASTER="yarn"
  val SPARK_EXECUTOR_CORES="10"
  val SPARK_EXECUTOR_MEMORY="4g"
  val SPARK_NETWORK_TIMOUT_TS="800s"
  val DB_NAME=""
  val JDBC_URL=""
  val FORMAT_TS = new SimpleDateFormat("MMddyyyyhhmmssSSS")
  val FORMAT_JOB_TS = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
}
