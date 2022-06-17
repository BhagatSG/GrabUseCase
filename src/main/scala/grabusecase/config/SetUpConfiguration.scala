package grabusecase.config

import grabusecase.entity.{ConfigObject, ConfigurationDetails}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by bhagat on 3/17/19.
  */
object SetUpConfiguration extends ConfigurationDetails{
 /**
    * Function to set the Spark Context objects
    *
    * @return Configuration Object
    */
  def getAllSparkConfig(appname:String):ConfigObject={
    try{
      val sparkConf=new SparkConf().setAppName(appname).setMaster("yarn")
      val spark = SparkSession.builder().enableHiveSupport().config("hive.exec.dynamic.partition", "true").config("hive.exec.dynamic.partition.mode", "nonstrict").config("spark.sql.hive.convertMetastoreOrc", "false").getOrCreate()
      //val ssc = new StreamingContext(spark.sparkContext,Seconds(pollingWindow))
      val configObject=new ConfigObject(sparkConf,spark)
      return configObject
    }
    catch {
      case textError: Throwable => textError.printStackTrace()
        sys.exit(1)
    }
  }

  /**
    * Function to set the Spark related properties
    *
    *
    * @param configObject		: Configuration Object
    */
  def Setproperties(configObject: ConfigObject)
  {
    configObject.spark.sparkContext.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    configObject.spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIAIWZIG6QJ6GMJTHFQ")
    configObject.spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "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")
    configObject.spark.sparkContext.hadoopConfiguration.set("fs.s3.canned.acl","BucketOwnerFullControl")

    configObject.sparkConf.set("spark.executor.cores", SPARK_EXECUTOR_CORES)
    configObject.sparkConf.set("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
    configObject.sparkConf.set("spark.network.timeout", SPARK_NETWORK_TIMOUT_TS)
    configObject.sparkConf.set("spark.sql.tungsten.enabled", "true")
    configObject.sparkConf.set("spark.eventLog.enabled", "true")
    configObject.sparkConf.set("spark.io.compression.codec", "snappy")
    configObject.sparkConf.set("spark.rdd.compress", "true")
    configObject.sparkConf.set("spark.dynamicAllocation.enabled", "true")
    configObject.sparkConf.set("spark.shuffle.service.enabled", "true")
    configObject.sparkConf.set("spark.streaming.kafka.maxRatePerPartition","10000")
    configObject.sparkConf.set("spark.streaming.concurrentJobs","6")
    configObject.sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    configObject.sparkConf.set("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
    configObject.sparkConf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")

  }

  /**
    * setup method invokes other methods for setup before job processing starts
    * @return : Configuration Object
    */
  def setup(appname :String):ConfigObject = {
    val config=SetUpConfiguration.getAllSparkConfig(appname)
    SetUpConfiguration.Setproperties(config)
    return config
  }
}

