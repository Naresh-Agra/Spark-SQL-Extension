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
    
    
    
    
    
  }
  
}