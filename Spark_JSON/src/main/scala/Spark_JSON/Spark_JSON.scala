package Spark_JSON
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

object Spark_JSON {
  
  def main(args:Array[String]):Unit={
    
    val conf=new SparkConf().setAppName("Spark_JSON").setMaster("local[*]").set("spark.driver.allowMultipleContexts","true")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark=SparkSession
              .builder()
              .config(conf)
              .getOrCreate()
              
    import spark.implicits._
    
    val devices_data=spark.read.format("json").load("file:///E://Workouts//Data//devices.json")
    devices_data.show(false)
    
    
   	// Invalid JSON
   	
    val jsonrdd=sc.textFile("file:///E://Workouts//Data//invalidjson.txt") //invalid multine json loading as rdd
    jsonrdd.foreach(println)
    println
    val replaced=jsonrdd.map(x=>x.replace("'", "\"")) // replacing single quotes with double quotes 
    replaced.foreach(println)

    val html=Source.fromURL("https://randomuser.me/api/0.8/?results=10")
    val urlstring=html.mkString
    val rddstring=sc.parallelize(List(urlstring))
    rddstring.foreach(println)
    val web_data=spark.read.json(rddstring)
    web_data.printSchema()
    println
    println("Explode Operation")
    val exploding = web_data.withColumn("results",explode($"results")).show(false)   
    println
    println("Without Explode")
    val without = web_data.select("results.user.username")
   	without.show(false)
   	println
    println("Lit Operation--- Adds new column")
    val lit_func = web_data.withColumn("TIme",lit(current_timestamp())).show() 
    println
    println("Processed Data")
    val processed_data1 = web_data.withColumn("processing",explode($"results")).selectExpr("processing.user.username as UserName","processing.user.email as EMail","processing.user.location.zip as ZipCode")
   	processed_data1.show(false)
   	processed_data1.printSchema()
   	


  }
  
}