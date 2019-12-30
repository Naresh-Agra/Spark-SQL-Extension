package Spark_SQL_Main
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql._

object Spark_SQL_Main {
  
  
  case class schema(first_name:String,last_name:String,company_name:String,address:String,city:String,county:String,state:String,zip:String,age:String,phone1:String,phone2:String,email:String,web:String)

  
  def main(args:Array[String])={
    
    val conf=new SparkConf().setAppName("SQL_Revise").setMaster("local[*]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR") // WARN, INFO, ERROR
    val spark=SparkSession
              .builder()
              .config(conf)
              .getOrCreate()
     
     import spark.implicits._
     
    val data_rdd=sc.textFile("file:///E://Workouts//Data//usdata1.csv", 1)
//    data_rdd.foreach(println)
    val data_df=spark.read.option("header", "true").format("csv").load("file:///E://Workouts//Data//usdata1.csv")
    data_df.show(false)
    println("select only the one column")
    data_df.select("first_name").show(false)
     println("Select everybody, but increment the age by 1")
     data_df.select($"first_name",$"age"+1).show()
     println("Select people older than 21")
     data_df.filter($"age">21).show()
    println("Count people by age")
    data_df.groupBy("age").count().show()    
    
    // Register the DataFrame as a SQL temporary view
    println("TempView") //Temporary views in Spark SQL are session-scoped and will disappear if the session that creates it terminates  
    data_df.createOrReplaceTempView("people")
      
      val SQLdf=spark.sql("select first_name,age from people")
       SQLdf.show()
       
       // Register the DataFrame as a global temporary view
         println("Global View") 
       data_df.createGlobalTempView("people")
       spark.sql("select * from global_temp.people").show()
       
       
       // Global temporary view is cross-session
        println("Global View in cross-session")
      spark.newSession().sql("SELECT * FROM global_temp.people").show()
       
      
      val usdata=sc.textFile("file:///E://Workouts//Data//usdata.csv", 1) // File RDD to fetch csv as input file
      val non_quote_data=usdata.filter(x=> !(x.contains("\""))) // Sub RDD -- Filtering data with no quotes 
      val quote_data=usdata.filter(x=>x.contains("\"")) // Sub RDD -- Filtering data with quotes
      
      // Assigning schema for non quote data
      println("Non Quoted Data")
       val schema_non_quote=non_quote_data.map(x=>x.split(",")).map(x=>schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12)))
        val non_quote_df=schema_non_quote.toDF()
        non_quote_df.show(false)
      // Assigning schema for quote data by concatnating company_name which is having comma in between
        println("Quoted Data")
       val schema_quote=quote_data.map(x=>x.split(",")).map(x=>schema(x(0),x(1),x(2)+","+x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13)))
      val quote_df=schema_quote.toDF()
        non_quote_df.show(false)
    // Using UNION -- comnine the data by using union
        println("Final Output DataFrame by using toDF()")
        val final_data=quote_df.union(non_quote_df)
        final_data.show(false)

       
       println("RDD to DF using struct")
       val struct=
         StructType(
             StructField("Name", StringType, true) ::
             StructField("Last_Name", StringType, false) ::
             StructField("Company", StringType, true)::
             StructField("Address", StringType, true)::
             StructField("City", StringType, false)::
             StructField("County", StringType, true)::
             StructField("State", StringType, true)::
             StructField("PinCode", StringType, true)::
             StructField("Age", StringType, false)::
             StructField("Phone_Number1", StringType, true)::
             StructField("Phone_Number2", StringType, true)::
             StructField("Mail_Id", StringType, true)::
             StructField("Web", StringType, false):: Nil)
               
             
             
      // Assigning row rdd for non quote data
       val row_non_quote=non_quote_data.map(x=>x.split(",")).map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12)))
       val row_non_quote_df=spark.createDataFrame(row_non_quote,struct)
       
      // Assigning row rdd for quote data by concatnating company_name which is having comma in between
       val row_quote=quote_data.map(x=>x.split(",")).map(x=>Row(x(0),x(1),x(2)+","+x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12),x(13)))
       val row_quote_df=spark.createDataFrame(row_quote,struct)
      
    
        // Using UNION -- combine the data by using union
        println("Final Output DataFrame by using struct")
        val final_data_struct=row_quote_df.union(row_non_quote_df)
        final_data_struct.show(false)            

       println("Showing SQL Operations")
       final_data_struct.createOrReplaceTempView("people")
       val sql_data=spark.sql("select Name,Last_Name,Company,Address,Age from people where Age>23")
       sql_data.show(false)
   //    sql_data.write.format("hive").mode("Append").saveAsTable("database.table")
      
       
       println("Converting DF to RDD")
       val rdd_data=sql_data.rdd
      rdd_data.take(5).foreach(println)
      

      
  }
  
  
}