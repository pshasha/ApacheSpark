package retail_db

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.typesafe.config._

object dailyRevenueusingDF {
  def main(args: Array[String]): Unit = {
    val props = ConfigFactory.load()
    val conf = new SparkConf().
      setMaster(props.getConfig(args(0)).getString("executionMode")).setAppName("revenueusingDF")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val ordersSchema = StructType(
        StructField("order_id",IntegerType,true) ::
        StructField("order_date",StringType,true)::
        StructField("order_customer_id",IntegerType,true)::
        StructField("order_status",StringType,true):: Nil)


    val orderItemsSchema = StructType(
      StructField("order_item_id",IntegerType,true) ::
        StructField("order_item_order_id",IntegerType,true) ::
        StructField("order_item_product_id",IntegerType,true) ::
        StructField("order_item_quantity",IntegerType,true) ::
        StructField("order_item_subtotal",FloatType,true)::
        StructField("order_item_price",FloatType,true):: Nil)

    sqlContext.setConf("spark.sql.shuffle.partitions","20")
    import sqlContext.implicits._

    val orders = sqlContext.read.schema(ordersSchema).
      format("csv").load("/home/shashank/retail_db/orders/part-00000")

    val order_items = sqlContext.read.schema(orderItemsSchema).format("csv").load("/home/shashank/retail_db/order_items/part-00000")

    val filterorders = orders.where("order_status in('CLOSED','COMPLETE')")

    val ordersNorderitems = filterorders.join(order_items,filterorders("order_id") === order_items("order_item_order_id"))

    val data = ordersNorderitems.select("order_id","order_date","order_status","order_item_subtotal")

    val datechange = data.withColumn("order_date",to_date(data("order_date")))


    val result = datechange.groupBy("order_date","order_status").agg(count("order_id").alias("total_orders"),round(sum("order_item_subtotal"),2)
      .alias("Revenue")).orderBy(col("order_date").desc,col("order_status"),col("total_orders"),col("Revenue").desc)


  }
}
