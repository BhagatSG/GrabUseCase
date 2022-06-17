package grabusecase.utils

import java.util.Properties

object AWSkeyPropertiesReader {
  def getAWSkeyConfig(): Map[String, String] = {

    val awsMap = collection.mutable.Map.empty[String, String]

    val prop = new Properties

    val input = AWSkeyPropertiesReader.getClass().getClassLoader().getResourceAsStream("configuration/AWSkeyConfig.properties");
    prop.load(input)

    awsMap.put("awsaccesskey", prop.getProperty("accesskey"))
    awsMap.put("awssecretaccesskey", prop.getProperty("secretaccesskey"))

    awsMap.toMap

  }
}