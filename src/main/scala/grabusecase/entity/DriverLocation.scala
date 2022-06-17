package grabusecase.entity

/**
  * Created by bhagat on 3/17/19.
  */
case class DriverLocation(var driverId: String,
                          var latitude: String,
                          var longitude: String,
                          var datetime: String) extends Serializable{

}
