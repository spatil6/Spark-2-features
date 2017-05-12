package core.learning


import org.apache.spark.sql.SparkSession
import org.apache.avro.ipc.specific.Person
import org.apache.spark.sql.catalog.Column
import org.apache.commons.math.stat.descriptive.summary.Sum
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import scala.collection.immutable.Stack
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf


case class SimpleTuple(id: Int, desc: String)
case class Bank(age: Integer, job: String, marital: String, education: String, balance: Integer)
object ExploreSpark {

  def main(args: Array[String]) {

    /* creation of Spark session x*/
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("Explore Spark")
      .enableHiveSupport()
      .getOrCreate();
  
    /* Need to import spark session implicitly to use dataset encoder functionalityd */
    import sparkSession.implicits._

    /*Old API to load CSV file and create "dataframe" using case class*/
    val bankText = sparkSession.sparkContext.textFile("/usr/bank.csv", 2)
    val bankDF = bankText.map(p => p.split(";")).filter(p => p(0) != "\"age\"")
      .map(p => new Bank(p(0).toInt, p(1).replaceAll("\"", ""), p(2).replaceAll("\"", ""), p(3).replaceAll("\"", ""),
        p(5).replaceAll("\"", "").toInt)).toDF

    /* Dataframe API to load text file and create data frame will return Row collection */
    val dfBank = sparkSession.sqlContext.read.options(Map("header" -> "true", "inferSchema" -> "true", "delimiter" -> ";"))
      .format("com.databricks.spark.csv").load("/usr/bank.csv")

    /*Spark 2.0 API for create Dataframe*/
    /* Create dataset  from scala collection*/
    val dataList = List(
      SimpleTuple(5, "abc"),
      SimpleTuple(6, "bcd"))

    val dsCollection = sparkSession.createDataset(dataList)

    /* Data pre-processing - removing unwanted column from dataframe*/
    val seq = Seq("default", "loan", "housing", "contact", "day", "month", "duration", "campaign", "pdays", "previous", "poutcome", "y")
    val filteredDF = dfBank.select(dfBank.columns.filter { p => !seq.contains(p) }.map(x => dfBank.col(x)): _*)
    filteredDF.coalesce(1).write.option("header", "true").csv("/usr/newbank.csv")

    /* Encoders are generally created automatically through implicits from a `SparkSession`, or can be
     explicitly created by calling static methods on [[Encoders]].*/
    /* Dataset with bank class type, internally it use Encoder,it create convert java object to byte code which is memory efficient.*/
    val dsBank = sparkSession.read.options(Map("header" -> "true", "inferSchema" -> "true", "delimiter" -> ";"))
      .csv("/usr/newbank.csv").as[Bank]

    dfBank.cache()
    /* dataframe vs dataset*/

    dsBank.map(_.age)
    dfBank.select("age")
    dsBank.persist(StorageLevel.MEMORY_ONLY_SER)
    dsBank.rdd.partitions(4)
    dsBank.repartition(3)
    
    dsBank.filter(p => p.age == 23) // Compile time saftey with column name and data type.
    dfBank.filter("age==23") // Error at runtime if wrong column name or data type.

    println(dsBank.queryExecution.optimizedPlan.numberedTreeString)
    println(dfBank.queryExecution.optimizedPlan.numberedTreeString)

    val mapPartitionsDs = dsBank.mapPartitions(k => List(k.count(value => true)).iterator)

    /*word cound with new dataset API*/
    val data = sparkSession.read.text("file:///hadoopdata/usr/local/temp1/sample.txt").as[String]
    val words = data.flatMap(value => value.split(","))
    val groupedWords = words.groupByKey(_.toLowerCase)
    groupedWords.count() //word count

    /*Reduce by key replaced by mapGroups*/
    groupedWords.mapGroups(fun(_, _)) // Word count
    def fun(a: String, b: Iterator[String]): (String, Int) =
      {
        (a, b.size)
      }

    /*Reduce operation in rdd and ds*/
    val rddReduce = dsBank.rdd.reduce((a, b) => a)
    dsBank.reduce(funcs(_, _))
    def funcs(b1: Bank, b2: Bank): Bank =
      {
        val a = new Bank(b1.age, b1.job, b1.marital, b1.education, b2.balance)
        return a
      }

    /*Aggregate function*/
    dsBank.agg(sum("age"))

    /*Windows function in spark*/
    val stocksDF = sparkSession.read.option("header", "true").
      option("inferSchema", "true")
      .csv("src/main/resources/applestock.csv")

    val stocks2016 = stocksDF.filter("year(Date)==2016")

    val tumblingWindowDS = stocks2016.groupBy(window(stocks2016.col("Date"), "1 week"))
      .agg(avg("Close").as("weekly_average"))
    println("weekly average in 2016 using tumbling window is")

    /*Catlog unction in spark , to access database, table and UDF name*/

    val databaseDS = sparkSession.catalog.listDatabases() // Returns database names

    dsBank.createTempView("Bank")

    sparkSession.catalog.dropTempView("Bank")

    dsBank.cache()

    sparkSession.catalog.isCached("Bank")
    
    /*Broadcast variable and HIVE UDF function*/
    /*Simple application for updating records in old dataframe from new dataframe*/
    
    /*sample old data */
    val firstdf = sparkSession.sparkContext.parallelize(Seq(("foo", 1), ("bar", 2),("koo",3),("soo",4))).toDF("k", "v")
    /*sample new data*/
    val newdf = sparkSession.sparkContext.parallelize(Seq(("fooooooooooooooooooooo", 1), ("barkkkk", 6))).toDF("k", "v")
    val seqdata= newdf.select("v").rdd.map(r=>r(0)).distinct().collect()
    /*broadcast new data*/
    val bc = sparkSession.sparkContext.broadcast(seqdata)
    /*UDF function to check new record is exist in old data*/
    val func: (Integer => Boolean) = (arg: Integer) => bc.value.contains(arg)
    val sqlfunc = udf(func)
    /*filter on old data*/
    val filtered =firstdf.filter(!sqlfunc(col("v")))
    /*result*/
    val finals =filtered.unionAll(newdf)
    
    /*Spark streaming*/
    val ssc = new StreamingContext(new SparkConf(),Seconds(2))
 

  }
}