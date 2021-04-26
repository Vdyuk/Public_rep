import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}


object filter extends App {

  val spark = SparkSession.builder()
    .appName("lab04a_dyukarev")
    //.config("spark.master", "local[*]")
    .getOrCreate()

  import spark.implicits._

  val topicName = spark.conf.get("spark.filter.topic_name")
  val outputDir = spark.conf.get("spark.filter.output_dir_prefix")
  val topicOffsetParam = spark.conf.get("spark.filter.offset")

  //val topicName = "lab04_input_data"
  //val topicOffsetParam = "earliest"

  val topicOffset = if (topicOffsetParam != "earliest") {
    s"""{"$topicName":{"0":$topicOffsetParam}}"""
  } else {
    "earliest"  }

  val kafkaParams = Map(
    "kafka.bootstrap.servers" -> "10.0.1.13:6667",
    "subscribe" -> topicName,
    "startingOffsets" -> topicOffset
  )
  val df = spark.read.format("kafka").options(kafkaParams).load

  val jsonString = df.select('value.cast("string")).as[String]

  val parsed = spark.read.json(jsonString)

  spark.conf.set("spark.sql.session.timeZone", "UTC")

  val parsed_UTC = parsed.withColumn("date", from_unixtime(col("timestamp")/1000,"yyyyMMdd"))

  // view
  parsed_UTC
    .filter('event_type === "view")
    .withColumn("$date", 'date)
    .write
    .mode("overwrite")
    .partitionBy("$date")
    .json(s"$outputDir/view")
    //.json("visits/view/")


  // buy
  parsed_UTC
    .filter('event_type === "buy")
    .withColumn("$date", 'date)
    .write
    .mode("overwrite")
    .partitionBy("$date")
    .json(s"$outputDir/buy/")
    //.json("visits/buy/")

}
