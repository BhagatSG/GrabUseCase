package grabusecase.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object SparkContextFactory {

  val spark = SparkSession.builder().appName("Grab-usecase").master("yarn").enableHiveSupport().getOrCreate()
  spark.sparkContext.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
  spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIAIKVBDNMTN6DCDF7Q")
  spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "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")
  spark.sparkContext.hadoopConfiguration.set("fs.s3.canned.acl","BucketOwnerFullControl")
  def getSparkContext(appname: String): SparkSession  = {
    return spark
  }
}
