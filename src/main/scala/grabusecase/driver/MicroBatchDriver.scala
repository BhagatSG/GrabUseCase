package grabusecase.driver

import com.amazonaws.{AmazonClientException, AmazonServiceException}
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{DeleteObjectRequest, ListObjectsRequest}
import grabusecase.config.SetUpConfiguration
import grabusecase.entity.{ConfigObject, DriverLocation}
import grabusecase.process.TransformationsHelper
import grabusecase.utils.AWSkeyPropertiesReader
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{StringType, StructType}

/**
  * Created by bhagat on 3/27/19.
  */
object MicroBatchDriver {
  val log = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val applicationName = "Grab-Usecase-MicroBatch-Driver"

    log.info("Spark Context set up #####################################")
    val configObject: ConfigObject = SetUpConfiguration.setup(applicationName)

    log.info("AWS S3 Configuration #####################################")
    val awsMap = AWSkeyPropertiesReader.getAWSkeyConfig()
    val credentials = new BasicAWSCredentials(awsMap.getOrElse("awsaccesskey", ""), awsMap.getOrElse("awssecretaccesskey", ""))
    val s3client = new AmazonS3Client(credentials)
    s3client.setRegion(Region.getRegion(Regions.US_WEST_2))

    log.info("Customer Demand Data Schema #####################################")
    val CustDemandSchema = (new StructType).add("cust_id", StringType).add("cust_latitude", StringType).add("cust_longitude", StringType).add("cust_datetime", StringType)
    log.info("Cabs Supply Data Schema #####################################")
    val CabSupplySchema = (new StructType).add("cab_id", StringType).add("cab_latitude", StringType).add("cab_longitude", StringType).add("cab_datetime", StringType)

    log.info("congestionEstimatorSchema #####################################")
    val congestionEstimatorSchema = (new StructType).add("trip_id", StringType).add("pickup_datetime", StringType).add("dropoff_datetime", StringType).add("pickup_longitude", StringType).add("pickup_latitude", StringType).add("dropoff_longitude", StringType).add("dropoff_latitude", StringType).add("trip_distance", StringType)

    val bucketName = "grab-realtime-data"
    var custDataKey = ""
    var custDataValue = ""

    var cabDataKey = ""
    var cabDataValue = ""

    var dataKey = ""
    var dataValue = ""

    try {

      val objectListing = s3client.listObjects(new ListObjectsRequest().withBucketName(bucketName))
      val iterator = objectListing.getObjectSummaries().iterator()

      while (iterator.hasNext()) {
        var objectSummary = iterator.next()

        if (objectSummary.getKey().contains("DemandData")) {
          custDataKey = objectSummary.getKey()
          custDataValue = "s3n://" + bucketName + "/" + objectSummary.getKey()
          log.info("dataKey ###### " + custDataKey + " ###### dataValue ###### " + custDataValue)
        }

        if (objectSummary.getKey().contains("SupplyData")) {
          cabDataKey = objectSummary.getKey()
          cabDataValue = "s3n://" + bucketName + "/" + objectSummary.getKey()
          log.info("dataKey ###### " + cabDataKey + " ###### dataValue ###### " + cabDataValue)
        }

        if (custDataKey != "" && cabDataKey != "") {
          val custDemandDF = configObject.spark.sqlContext.read.format("csv").option("header", "true")
            .schema(CustDemandSchema).load(custDataValue)
          log.info("custRqstDF count " + custDemandDF.count())

          val cabSupplyDF = configObject.spark.sqlContext.read.format("csv").option("header", "true")
            .schema(CabSupplySchema).load(cabDataValue)
          log.info("cabAvlbleDF count " + cabSupplyDF.count())

          //val cabSupplyDS = cabSupplyDF.as[DriverLocation]

          val success = TransformationsHelper demandSupplyRatioCalculator(custDemandDF, cabSupplyDF, configObject, this.getClass.getSimpleName)
          log.info("TransformationsHelper demandSupplyRatioCalculator Success " + success)

          if (success == true) {
            log.info("Deleting Object & variable re-initialisation" + success)
            //s3client.deleteObject(new DeleteObjectRequest(bucketName, custDataKey))
            //s3client.deleteObject(new DeleteObjectRequest(bucketName, cabDataKey))

            custDataKey = ""
            custDataValue = ""

            cabDataKey = ""
            cabDataValue = ""
          }
        }

        if (objectSummary.getKey().contains("TrafficEstimator")) {
          dataKey = objectSummary.getKey()
          dataValue = "s3n://" + bucketName + "/" + objectSummary.getKey()
          log.info("dataKey ###### " + dataKey + " ###### dataValue ###### " + dataValue)


          val congestionEstimatorDF = configObject.spark.sqlContext.read.format("csv").option("header", "true")
            .schema(congestionEstimatorSchema).load(dataValue)
          log.info("congestionEstimatorDF count " + congestionEstimatorDF.count())

          val success = TransformationsHelper traffiCongestionEstimator(congestionEstimatorDF, configObject, this.getClass.getSimpleName)
          log.info("TransformationsHelper traffiCongestionEstimator Success " + success)

          if (success == true) {
            log.info("Deleting Object & variable re-initialisation" + success)
            //s3client.deleteObject(new DeleteObjectRequest(bucketName, dataKey))
            dataKey = ""
            dataValue = ""
          }
        }

      }
    } catch {
      case ase: AmazonServiceException => {
        log.error(ase.printStackTrace().toString)
      }
      case ace: AmazonClientException => {
        log.error(ace.getStackTrace().toString)
      }
    }

  }

}
