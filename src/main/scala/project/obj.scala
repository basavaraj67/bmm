package project

import scala.io.Source

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.current_date
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

object obj {

  def addColumnIndex(spark: SparkSession, df: DataFrame) = {
    spark.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map {
        case (row, index) => Row.fromSeq(row.toSeq :+ index + 1)
      },

      StructType(df.schema.fields :+ StructField("id", LongType, false)))
  }

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    println("================Started1============")

    val conf = new SparkConf().setAppName("revision").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
   
					
					println("======================= Step 2 ========Raw data=============================================")
					


					val data = spark.read.format("com.databricks.spark.avro")
					.load("file:///M://data//projectsample.avro")

					data.show()




					
					


    
  }
}
