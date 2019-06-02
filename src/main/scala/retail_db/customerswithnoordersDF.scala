package retail_db

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.typesafe.config._

object customerswithnoordersDF {
  def main(args: Array[String]): Unit = {
    val props = ConfigFactory.load()
    val conf = new SparkConf().setAppName("customerNoorders").
      setMaster(props.getConfig(args(0)).getString("executionMode"))
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val ordersSchema = StructType(
      StructField("order_id",IntegerType,true)::
        StructField("order_date",StringType,true) ::
        StructField("order_customer_id",IntegerType,true) ::
        StructField("order_status",StringType,true):: Nil)

    val customersSchema = StructType(
      StructField("customer_id",IntegerType,true) ::
        StructField("customer_fname",StringType,true) ::
        StructField("customer_lname",StringType,true) ::
        StructField("customer_email",StringType,true) ::
        StructField("customer_password",StringType,true) ::
        StructField("customer_street",StringType,true) ::
        StructField("customer_city",StringType,true) ::
        StructField("customer_state",StringType,true) ::
        StructField("customer_zipcode",StringType,true) :: Nil)


    sqlContext.setConf("spark.sql.shuffle.partitions","20")
    val path = args(1)


    val orders = sqlContext.read.format("csv").schema(ordersSchema).load(s"${path}/orders/part-00000")
    val customers = sqlContext.read.format("csv").schema(customersSchema).load(s"${path}/customers/part-00000")


    val result = customers.join(orders,customers("customer_id") === orders("order_customer_id"),"left_outer")

    val data = result.select("customer_lname","customer_fname","order_id")

    val lastresult = data.where("order_id is Null").drop("order_id").
      orderBy(col("customer_lname").desc,col("customer_fname").desc)


  }

}
