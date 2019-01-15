package retail_db

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.typesafe.config._
import org.apache.hadoop.hdfs.DFSClient.Conf

object productusingDF {
  def main(args: Array[String]): Unit = {
    val props = ConfigFactory.load()
    val conf = new SparkConf().
      setAppName("productusingDF").
      setMaster(props.getConfig(args(0)).getString("executionMode"))
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    sqlContext.setConf("spark.sql.shuffle.partitions","20")
 import sqlContext.implicits._

    val productSchema = StructType(
      StructField("product_id",IntegerType,true) ::
        StructField("product_category_id",IntegerType,true) ::
        StructField("product_name",StringType,true) ::
        StructField("product_description",StringType,true) ::
        StructField("product_price",FloatType,true) ::
        StructField("product_image",StringType,true) :: Nil)

    val products = sqlContext.read.schema(productSchema).format("csv").load("/home/shashank/retail_db/products/part-00000")

    val filter_price = products.where("product_price < 100")

    val result = filter_price.groupBy("product_category_id").agg(max("product_price").
      alias("Max_product_price"),min("product_price").alias("Min_product_price"),round(avg("product_price"),2)
      .alias("Avg_product_price"),countDistinct("product_id").alias("total_product")).orderBy("product_category_id")


    result.show()






  }
}
