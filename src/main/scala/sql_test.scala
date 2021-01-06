import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SQLTest extends App with Logging {
  Logger.getLogger("org").setLevel(Level.WARN)
  val sparkConf = new SparkConf()
    .set("spark.driver.cores", "2")
    .setAppName(this.getClass.getSimpleName)
    .setMaster("local[*]")

  // create spark session
  val spark: SparkSession = SparkSession.builder().config(sparkConf).appName(this.getClass.getName)
    .master("local[*]").getOrCreate()
  spark.sparkContext.getConf.getAll.foreach(println)

  import spark.implicits._

  // load customer data
  val dfcustomer = spark.read.format("csv").option("header","true").option("inferSchema","true").load("./data/customer/*")
  dfcustomer.show(false)
  dfcustomer.printSchema()


  // load sales data & add new column created_ts which is casted to timestamp
  val dfsales = spark.read.format("csv").option("header","true").option("inferSchema","true").load("./data/sales/*")
    .selectExpr("*", "cast(sales_ts as timestamp) created_ts" )
  dfsales.show(false)
  dfsales.printSchema()

  // add additional columns in the data frame
  val dfsalesadditional = dfsales.selectExpr("*"
    , "year(created_ts) year"
    , "month(created_ts) month"
    , "day(created_ts) day"
    , "hour(created_ts) hour")

  dfsalesadditional.show(false)
  dfsalesadditional.printSchema()

  // create a new dataframe by joining two datasets on column (dfcustomer.id and dfsalesadditional.customer_id
  val dfjoin = dfcustomer.join(dfsalesadditional,dfcustomer("id")===dfsalesadditional("customer_id"))
  dfjoin.show()

  /**
  dfjoin.createOrReplaceTempView("customer_sales")
  spark.sql("select state, year, month, day, hour, sum(sales_amount) from customer_sales group by state, year, month, day, hour " +
    "WITH ROLLUP order by state asc nulls last, year desc nulls last, month desc nulls last, day asc nulls last, hour desc nulls last")
    .show(false)
   **/

  // create a new dataframe by applying rollup, agg, sort
  val withRollup = dfjoin
    .rollup("state", "year", "month", "day", "hour")
    .agg(sum("sales_amount") as "amount", grouping_id() as "gid")
    .sort($"state".asc_nulls_last, $"year".desc_nulls_last, $"month".desc_nulls_last, $"day".asc_nulls_last, $"hour".desc_nulls_last)
    .select("state", "year", "month", "day", "hour" , "amount")
  withRollup.show(10000,false)

  withRollup
    .coalesce(1)
    .write
    .option("header", "true")
    .option("sep", "#")
    .option("encoding", "UTF-8")
    .mode("overwrite")
    .option("nullValue", "")
    .csv("./data/results/output.txt")

}