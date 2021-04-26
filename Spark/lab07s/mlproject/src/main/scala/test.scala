import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructField, StructType}

object test {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("lab07-test").getOrCreate()
    import spark.implicits._

    val sparkConf: Map[String, String] = spark.sqlContext.getAllConfs
    val pathToModel = sparkConf.getOrElse("spark.model_path", "/user/vladimir.dyukarev/lab07/model")
    val kafkaServer = sparkConf.getOrElse("spark.kafka_host", "10.0.1.13:6667")
    val inputTopic = sparkConf.getOrElse("spark.input_topic", "vladimir_dyukarev")
    val checkpointPath = sparkConf.getOrElse("spark.checkpointPath", "/user/vladimir.dyukarev/lab07/chk/out")
    val outputTopic = sparkConf.getOrElse("spark.output_topic", "vladimir_dyukarev_lab04b_out")

    val inputStream = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", inputTopic)
      .option("startingOffsets", "earliest")
      .load()


    val jsonSchema = new StructType()
      .add("uid", StringType)
      .add("visits", ArrayType(StructType(Array(
        StructField("timestamp", LongType),
        StructField("url", StringType))
      )))

    val df2 = inputStream
      .select(from_json(col("value").cast("string"), jsonSchema).as("jsonData"))
      .select("jsonData.*")
      .withColumn("visit", explode($"visits"))
      .withColumn("host", lower(callUDF("parse_url", $"visit.url", lit("HOST"))))
      .withColumn("domain", regexp_replace($"host", "www.", ""))
      .groupBy("uid").agg(collect_list("domain") as "domain_features")
      .select("uid", "domain_features")

    val model = PipelineModel.load(pathToModel)

    val outputStream = model.transform(df2)
        .withColumnRenamed("prediction_gender_age", "gender_age")
        .select("uid", "gender_age")


    outputStream.toJSON
      .withColumn("topic", lit(outputTopic))
      .writeStream.format("kafka")
      .option("checkpointLocation", checkpointPath)
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("topic", outputTopic)
      .start()
      .awaitTermination()

    spark.stop()
  }
}


