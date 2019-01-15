package retail_db

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.typesafe.config._

object dailyrevneueperdepartmentDF {
  def main(args: Array[String]): Unit = {

    val props = ConfigFactory.load()
    val conf = new SparkConf().
      setMaster(props.getConfig(args(0)).getString("executionMode")).
      setAppName("revenueperdepartment")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val ordersSchema = StructType(
      StructField("order_id",IntegerType,true)::
        StructField("order_date",StringType,true) ::
        StructField("order_customer_id",IntegerType,true) ::
        StructField("order_status",StringType,true):: Nil)

    val orderItemsSchema = StructType(
      StructField("order_item_id",IntegerType,true) ::
        StructField("order_item_order_id",IntegerType,true) ::
        StructField("order_item_product_id",IntegerType,true) ::
        StructField("order_item_quantity",IntegerType,true) ::
        StructField("order_item_subtotal",FloatType,true)::
        StructField("order_item_price",FloatType,true):: Nil)

    val productsSchema = StructType(
      StructField("product_id",IntegerType,true) ::
        StructField("product_category_id",IntegerType,true) ::
        StructField("product_name",StringType,true) ::
        StructField("product_description",StringType,true) ::
        StructField("product_price",FloatType,true) ::
        StructField("product_image",StringType,true) :: Nil)

    val categorySchema = StructType(
      StructField("category_id",IntegerType,true)::
      StructField("category_department_id",IntegerType,true)::
      StructField("category_name",StringType,true):: Nil)

    val departmentSchema = StructType(
      StructField("department_id",IntegerType,true)::
      StructField("department_name",StringType,true)::Nil)

    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions","100")
    val path = args(1)

    val orders = sqlContext.read.format("csv").schema(ordersSchema).load(s"${path}/orders/part-00000")
      val orderItems = sqlContext.read.format("csv").schema(orderItemsSchema).load(s"${path}/order_items/part-00000")
        val products = sqlContext.read.format("csv").schema(productsSchema).load(s"${path}/products/part-00000")
          val category = sqlContext.read.format("csv").schema(categorySchema).load(s"${path}/categories/part-00000")
            val department = sqlContext.read.format("csv").schema(departmentSchema).load(s"${path}/departments/part-00000")


    val ordersFilterdata = orders.where("order_status in ('CLOSED','COMPLETE')")

    //joining datasets

    val ordersNorderitems = ordersFilterdata.join(orderItems,ordersFilterdata("order_id") === orderItems("order_item_order_id"))

    val ordersNorderitemsNoproducts = ordersNorderitems.join(products,ordersNorderitems("order_item_product_id") === products("product_id"))

    val ordersNorderitemsNoproductsNcategory = ordersNorderitemsNoproducts.
      join(category,ordersNorderitemsNoproducts("product_category_id") === category("category_id"))

    val lastdata = ordersNorderitemsNoproductsNcategory.
      join(department,ordersNorderitemsNoproductsNcategory("category_department_id") === department("department_id"))

    val data = lastdata.select("order_date","order_item_subtotal","department_name")


    val resultset = data.groupBy(to_date(col("order_date")).alias("order_date"),col("department_name").alias("department_name")).
      agg(round(sum(col("order_item_subtotal")),2).alias("Revenue"))

    resultset.show(1)


  }

}
