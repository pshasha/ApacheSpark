package flight_project

import akka.actor.FSM.->
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.typesafe.config._
import org.apache.spark.sql.hive._

object loadinHive {

  def main(args: Array[String]): Unit = {
    val props = ConfigFactory.load()
    val conf = new SparkConf().setMaster(props.getConfig(args(0)).getString("executionMode"))
      .setAppName("flight_project")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    val dataschema = StructType(
      StructField("Standard", IntegerType, true) ::
        StructField("Year", IntegerType, true) ::
        StructField("Month", IntegerType, true) ::
        StructField("DayofMonth", IntegerType, true) ::
        StructField("DayOfWeek", IntegerType, true) ::
        StructField("DepTime", FloatType, true) ::
        StructField("CRSDepTime", FloatType, true) ::
        StructField("ArrTime", FloatType, true) ::
        StructField("CRSArrTime", FloatType, true) ::
        StructField("UniqueCarrier", StringType, true) ::
        StructField("FlightNum", IntegerType, true) ::
        StructField("TailNum", StringType, true) ::
        StructField("ActualElapsedTime", FloatType, true) ::
        StructField("CRSElapsedTime", FloatType, true) ::
        StructField("AirTime", FloatType, true) ::
        StructField("ArrDelay", FloatType, true) ::
        StructField("DepDelay", FloatType, true) ::
        StructField("Origin", StringType, true) ::
        StructField("Dest", StringType, true) ::
        StructField("Distance", IntegerType, true) ::
        StructField("TaxiIn", FloatType, true) ::
        StructField("TaxiOut", FloatType, true) ::
        StructField("Cancelled", IntegerType, true) ::
        StructField("CancellationCode", StringType, true) ::
        StructField("Diverted", IntegerType, true) ::
        StructField("CarrierDelay", FloatType, true) ::
        StructField("WeatherDelay", FloatType, true) ::
        StructField("NASDelay", FloatType, true) ::
        StructField("SecurityDelay", FloatType, true) ::
        StructField("LateAircraftDelay", FloatType, true) :: Nil)

    val data = sqlContext.read.format("csv").option("header", "true").
      schema(dataschema).load(args(1))
    

    val removeNulls = data.na.fill(0.0)

    sqlContext.sql("use shashank5")

    removeNulls.registerTempTable("testingTable")

    sqlContext.sql("create table flight_project as select * from testingTable")



  }
}
