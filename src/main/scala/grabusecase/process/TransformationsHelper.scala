package grabusecase.process

import grabusecase.driver.{CongestionEstimatorDriver, DemandSupplyRatioCalDriver}
import grabusecase.entity.{ConfigObject, DriverLocation}
import grabusecase.utils.Utilities
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{udf, _}

/**
  * Created by bhagat on 3/22/19.
  */
object TransformationsHelper {
  val logger = Logger.getLogger(getClass.getName)

  def traffiCongestionEstimator(df: DataFrame, configObject: ConfigObject, driverName:String): Boolean = {

    logger.info("traffiCongestionEstimator UDF Initialisation")
    val tripTimeDifferenceUDF = udf(Utilities.tripTimeDifference _)
    val generateGeohashUDF = udf(Utilities.generateGeohash _)
    val getHourIntervalUDF = udf(Utilities.getHourInterval _)
    val getMinIntervalUDF = udf(Utilities.getMinInterval _)

    logger.info("generateGeohashUDF Call ")
    val geohashDF = df.withColumn("geohash_pickup", generateGeohashUDF(df("pickup_latitude"), df("pickup_longitude")))
      .withColumn("geohash_dropoff", generateGeohashUDF(df("dropoff_latitude"), df("dropoff_longitude")))
    //val dropoffDF = pickupDF.withColumn("geohash_dropoff", generateGeohashUDF(pickupDF("dropoff_latitude"), pickupDF("dropoff_longitude")))


    logger.info("TimeDifferenceUDF Call ")
    val timeDifferenceDF = geohashDF.withColumn("trip_time", tripTimeDifferenceUDF(geohashDF("pickup_datetime"), geohashDF("dropoff_datetime")))

    val filteredDF = timeDifferenceDF.filter(timeDifferenceDF("geohash_pickup").isNotNull).filter(timeDifferenceDF("geohash_dropoff").isNotNull).filter(timeDifferenceDF("trip_time").isNotNull).filter(timeDifferenceDF("trip_distance").isNotNull)


    logger.info("Speed Calculation & Average Speed")
    val tripDF = filteredDF.withColumn("speed", (filteredDF("trip_distance") * 60 / filteredDF("trip_time")))
    val average_speed = tripDF.agg(avg(tripDF("speed"))).take(1)(0).getDouble(0)
    val finalDF = tripDF.withColumn("average_speed", lit(average_speed))


    logger.info("Dataframe Calculation for Pickup & Dropoff Data")
    val startTripDF = finalDF.select("geohash_pickup", "speed", "pickup_datetime", "average_speed").withColumnRenamed("geohash_pickup", "geohash").withColumnRenamed("pickup_datetime", "datetime")
    val endTripDF = finalDF.select("geohash_dropoff", "speed", "dropoff_datetime", "average_speed").withColumnRenamed("geohash_dropoff", "geohash").withColumnRenamed("dropoff_datetime", "datetime")


    logger.info("startTripDF Union with endTripDF")
    var combinedDF = startTripDF.unionAll(endTripDF)


    logger.info("Date, Hour Interval & Minute Interval Calculations ")
    combinedDF = combinedDF.withColumn("date", to_date(combinedDF("datetime")))
      .withColumn("hour_interval", getHourIntervalUDF(combinedDF("datetime")))
      .withColumn("min_interval", getMinIntervalUDF(combinedDF("datetime")))


    logger.info("Geo_Hash Average Speed Calculation")
    combinedDF = combinedDF.groupBy("geohash", "date", "hour_interval", "min_interval", "average_speed").agg(mean("speed")).withColumnRenamed("avg(speed)", "geohash_avg_speed")


    logger.info("Traffic Congestion Calculation based on Geo_Hash Average Speed")
    val trafficCongestionEstimatorDF = combinedDF.withColumn("congestion_status", when(combinedDF("geohash_avg_speed") > (combinedDF("average_speed") + 5), "Green")
      .when(combinedDF("geohash_avg_speed") < (combinedDF("average_speed") - 1), "Red").otherwise("Yellow"))

    /*if(driverName.equalsIgnoreCase(CongestionEstimatorDriver.getClass.getSimpleName)){
      logger.info("Traffic Congestion Estimation DF Write to Hive Table")
      Utilities.storeDataFrame(trafficCongestionEstimatorDF,"Append","ORC","grabusecase.traffic_congestion_estimation")
    } else {
      logger.info("Traffic Congestion Estimation DF Write to S3")
      trafficCongestionEstimatorDF.repartition(1).write.format("csv").option("header", "true").mode("Overwrite")
        .save("s3n://grab-extract-data/TrafficCongestionEstimatorMapview/")
      logger.info("Traffic Congestion Estimation DF Write to S3 Completed")
    }*/

    trafficCongestionEstimatorDF.repartition(1).write.format("csv").option("header", "true").mode("Overwrite")
      .save("s3n://grab-extract-data/TrafficCongestionEstimatorMapview/")
    //Utilities.storeDataFrameS3()

    return true
  }

  def demandSupplyRatioCalculator(custDF: DataFrame, cabDF: DataFrame, configObject: ConfigObject, driverName: String): Boolean = {

    logger.info("demandSupplyRatioCalculator UDF Initialisation")
    val generateGeohashUDF = udf(Utilities.generateGeohash _)
    val getHourIntervalUDF = udf(Utilities.getHourInterval _)
    val getMinIntervalUDF = udf(Utilities.getMinInterval _)


    logger.info("generateGeohashUDF Call ")
    var custDemandDF = custDF.withColumn("geohash", generateGeohashUDF(custDF("cust_latitude"), custDF("cust_longitude")))
    var cabSupplyDF = cabDF.withColumn("geohash", generateGeohashUDF(cabDF("cab_latitude"), cabDF("cab_longitude")))


    logger.info("Date, Hour Interval & Minute Interval Calculations ")
    custDemandDF = custDemandDF.withColumn("date", to_date(custDemandDF("cust_datetime")))
      .withColumn("hour_interval", getHourIntervalUDF(custDemandDF("cust_datetime")))
      .withColumn("min_interval", getMinIntervalUDF(custDemandDF("cust_datetime")))

    cabSupplyDF = cabSupplyDF.withColumn("date", to_date(cabSupplyDF("cab_datetime")))
      .withColumn("hour_interval", getHourIntervalUDF(cabSupplyDF("cab_datetime")))
      .withColumn("min_interval", getMinIntervalUDF(cabSupplyDF("cab_datetime")))


    logger.info("Calculations of count of Supply & Demand for specific Geohash, Hour_Interval & Minute_Interval")
    val custDemandCountDF = custDemandDF.groupBy("geohash", "date", "hour_interval", "min_interval").count().withColumnRenamed("count", "cust_demand_count")
    val cabSupplyCountDF = cabSupplyDF.groupBy("geohash", "date", "hour_interval", "min_interval").count().withColumnRenamed("count", "cab_supply_count")

    val demandSupplyDF = custDemandCountDF.join(cabSupplyCountDF,
      custDemandCountDF("geohash") <=> cabSupplyCountDF("geohash") && custDemandCountDF("date") <=> cabSupplyCountDF("date")
        && custDemandCountDF("hour_interval") <=> cabSupplyCountDF("hour_interval") && custDemandCountDF("min_interval") <=> cabSupplyCountDF("min_interval"), "full")
      .withColumn("demand_supply_ratio", custDemandCountDF("cust_demand_count") / cabSupplyCountDF("cab_supply_count"))
      .drop(cabSupplyCountDF("geohash")).drop(cabSupplyCountDF("date")).drop(cabSupplyCountDF("hour_interval")).drop(cabSupplyCountDF("min_interval"))

    /*if(driverName.equalsIgnoreCase(DemandSupplyRatioCalDriver.getClass.getSimpleName)){
      logger.info("Supply & Demand ratio DF Write to Hive Table")
      Utilities.storeDataFrame(demandSupplyDF,"Append","ORC","grabusecase.demand_supply_ratio")
    }
    else {
      logger.info("Demand Supply Ratio Calculation DF Write to S3")
      demandSupplyDF.repartition(1).write.format("csv").option("header", "true").mode("Overwrite")
        .save("s3n://grab-extract-data/DemandSupplyCalMapview/")
      logger.info("Demand Supply Ratio Calculation DF Write to S3 Completed")
    }*/

    demandSupplyDF.repartition(1).write.format("csv").option("header", "true").mode("Overwrite")
      .save("s3n://grab-extract-data/DemandSupplyCalMapview/")
    //Utilities.storeDataFrameS3()
    return true
  }

}
