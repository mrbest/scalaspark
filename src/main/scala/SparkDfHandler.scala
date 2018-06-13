import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkDfHandler extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  /*  val lines = spark.sparkContext.parallelize(
      Seq("Spark Intellij Idea Scala test one",
        "Spark Intellij Idea Scala test two",
        "Spark Intellij Idea Scala test three"))

    val counts = lines
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    counts.foreach(println) */

  val input = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", "\t").csv("/Users/cliftonbest/Documents/Development/R/GWCM_Exports/GWCM_FPDS_CURRENT_MONTH_JUNE_2018.tsv")

  println("there are "+input.count() + " rows")
  //println(input.first() + " row")
  //println( input.groupBy("TOTAL_PRICE").sum() + " total")

  //val input = spark.read.option("header", "true").option("sep", "\t").csv("/Users/destiny/Documents/development/GWCM_Exports/GWCM_FPDS_CURRENT_MONTH_JUNE_2018.tsv")
  input.printSchema()
  println("Obligated dollars "+ input.filter($"date_signed" >= "2017-10-01").agg(sum($"dollars_obligated")).show() )

}