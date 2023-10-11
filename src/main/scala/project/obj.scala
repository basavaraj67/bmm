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




					
					println("======================== Step 3 ========Url data=============================================")
					

					val html = Source.fromURL("https://randomuser.me/api/0.8/?results=500")
					val s = html.mkString
					//println(s)


					val urldf = spark.read.json(sc.parallelize(List(s)))
					urldf.show()




					
					println("========================step 4 flatten dataframe=============================================")
					
					val flatdf = urldf.withColumn("results",explode(col("results"))).select("nationality","seed","version",
							"results.user.username","results.user.cell","results.user.dob","results.user.email",
							"results.user.gender","results.user.location.city","results.user.location.state",
							"results.user.location.street","results.user.location.zip","results.user.md5",
							"results.user.name.first","results.user.name.last","results.user.name.title",
							"results.user.password","results.user.phone","results.user.picture.large","results.user.picture.medium"
							,"results.user.picture.thumbnail","results.user.registered","results.user.salt","results.user.sha1"
							,"results.user.sha256")
					flatdf.show()






					
					println("========================step 5 removed numericals Dataframe=============================================")
					
					val rm=flatdf.withColumn("username",regexp_replace(col("username"),  "([0-9])", ""))
					rm.show()







					
					println("====================== Step 6 =========Joined Dataframe=============================================")
					
					val joindf = data.join(broadcast(rm),Seq("username"),"left")


					joindf.show()


					
					println("=================== Step 7 a ============Not available customers=============================================")
					


					val dfnull = joindf.filter(col("nationality").isNull)


					val dfnotnull=joindf.filter(col("nationality").isNotNull)





					


					
					println("==================  Step 7 b =============available customers=============================================")
					




					dfnotnull.show()

					dfnull.show()



					
					println("=============== Step 8 ================Null handled dataframe=============================================")
					


					val replacenull= dfnull.na.fill("Not Available").na.fill(0)
					replacenull.show()


					
					println("=============== Step 9 a ================not available customers with current date dataframe=============================================")
					



					val replacenull_with_current_date=replacenull.withColumn("current_date",current_date)

					replacenull_with_current_date.show()

				
					println("=============== Step 9 b ================available customers with current date dataframe=============================================")
					



					val notnull_with_current_date=dfnotnull.withColumn("current_date",current_date)


					notnull_with_current_date.show()



    
  }
}
