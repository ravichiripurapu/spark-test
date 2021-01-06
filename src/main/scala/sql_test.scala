import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{split, _}
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

  // val dfsales1 = spark.read.format("csv").option("header","true").option("inferSchema","true").load("./data/sales/*")
  //  .selectExpr("*", "cast(sales_ts as timestamp) created_ts" )


  // add additional columns in the data frame
  // val dfsalesadditional = dfsales1.selectExpr("*", "year(created_ts) year", "month(created_ts) month", "day(created_ts) day", "hour(created_ts) hour")
  // dfsalesadditional.show(false)
  // dfsalesadditional.printSchema()


  // build dfsales with builder pattern in a single statement.
  var dfsales = spark
    .read
    .format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load("./data/sales/*")
    .withColumn("created_ts", col("sales_ts").cast("timestamp") )
    //.withColumn("year1", year(col("created_ts")))
    //.withColumn("month1", month(col("created_ts")))
    //.withColumn("day1", dayofmonth(col("created_ts")))
    //.withColumn("hour1", hour(col("created_ts")))
  dfsales.show(false)
  dfsales.printSchema()

  val split_col = split(dfsales("created_ts")," ")

  dfsales = dfsales
    .withColumn("created_dt", split_col.getItem(0))
    .withColumn("created_time", split_col.getItem(1))

  val split_dt = split(dfsales("created_dt"),"-")

  dfsales = dfsales
    .withColumn("year", split_dt.getItem(0))
    .withColumn("month", split_dt.getItem(1))
    .withColumn("day", split_dt.getItem(2))

  val split_ts = split(dfsales("created_time"),":")

  dfsales = dfsales
    .withColumn("hour", split_ts.getItem(0))

  // create a new dataframe by joining two datasets on column (dfcustomer.id and dfsalesadditional.customer_id
  val dfjoin = dfcustomer.join(dfsales,dfcustomer("id")===dfsales("customer_id"))
  dfjoin.show()

  /*
  dfjoin.createOrReplaceTempView("customer_sales")
  spark.sql("select state, year, month, day, hour, sum(sales_amount) from customer_sales group by state, year, month, day, hour " +
    "WITH ROLLUP order by state asc nulls last, year desc nulls last, month desc nulls last, day asc nulls last, hour desc nulls last")
    .show(false)
   */

  // create a new dataframe by applying rollup, agg, sort
  val withRollup = dfjoin
    .rollup("state", "year", "month", "day", "hour")
    .agg(sum("sales_amount") as "amount", grouping_id() as "gid")
    .sort($"state".asc_nulls_last, $"year".desc_nulls_last, $"month".desc_nulls_last, $"day".asc_nulls_last, $"hour".desc_nulls_last)
    .select("state", "year", "month", "day", "hour" , "amount")
  withRollup.show(2000,false)

  withRollup
    .coalesce(1)
    .write
    .option("header", "true")
    .option("sep", "#")
    .option("encoding", "UTF-8")
    .mode("overwrite")
    .option("nullValue", null)
    .csv("./data/results/output.txt")

}