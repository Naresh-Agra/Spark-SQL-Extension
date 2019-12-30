package Spark_SQL_Extension
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.jdbc._
import scala.io.Source
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions._
import com.mysql.cj.jdbc.Driver

object Spark_SQL_Extension {
  
  def main(args:Array[String]):Unit={
    
    val conf=new SparkConf().setAppName("Spark_JSON").setMaster("local[*]").set("spark.driver.allowMultipleContexts","true")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark=SparkSession
              .builder()
              .config(conf)
              .getOrCreate()
              
    import spark.implicits._
    
    val html=Source.fromURL("https://randomuser.me/api/0.8/?results=10") // Url buffered source
    val urlstring=html.mkString //making url as string
    val rddstring=sc.parallelize(List(urlstring)) //converting string to rdd
    val web_data=spark.read.json(rddstring) //converting rdd to dataframe as json
    web_data.show()
    web_data.printSchema()

    val processed_data1=web_data.withColumn("naresh", explode($"results")).selectExpr("naresh.user.name.first as Name","naresh.user.location.city as City","naresh.user.username as UserName","naresh.user.picture.thumbnail as Thumnail")
    println
    println("Reading data as DSL Operations")
    processed_data1.show(false)
    println
    println("Reading as SQL Operations")
    processed_data1.createOrReplaceTempView("processed")
    spark.sql("select * from processed").show(false)
    
    println
    println("Writing table content to local")
    processed_data1.write.format("csv").mode("append").save("file:///E://Workouts//Executed_Results//sql_ext1")
    println("Saved")
    
    
  }
  
}