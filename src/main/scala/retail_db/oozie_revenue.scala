package retail_db

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.typesafe.config._


object oozie_revenue {

  def main(args: Array[String]): Unit = {

    val props = ConfigFactory.load()
    val conf = new SparkConf().setMaster(props.getConfig(args(0)).getString("executionMode")).setAppName("oozie_revenue")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    sqlContext.setConf("spark.sql.shuffle.partitions","20")


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



    val orders = sqlContext.read.format("csv").schema(ordersSchema).load(args(1))
    val order_items = sqlContext.read.format("csv").schema(orderItemsSchema).load(args(2))

    val data = orders.join(order_items,orders("order_id") === order_items("order_item_order_id")).select("order_date","order_item_subtotal")


    val result = data.groupBy(col("order_date").alias("order_date")).
      agg(round(sum(col("order_item_subtotal")),1).alias("Revenue"))

    result.coalesce(1).write.format("csv").save(args(3))



  }
}
