import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, IndexToString, StringIndexer}
import org.apache.spark.ml.Pipeline

object train extends App {

  override def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("lab07-train").getOrCreate()

    import spark.implicits._

    //spark.conf.set("spark.sql.shuffle.partitions", 50)
    val sparkConf: Map[String, String] = spark.sqlContext.getAllConfs
    val inputPath = sparkConf.getOrElse("spark.train.path", "/labs/laba07")
    val OutputPath = sparkConf("spark.output.path")

    val weblog = spark.read.json(inputPath)

    val df = weblog
      .filter(col("uid").isNotNull)
      .withColumn("visit", explode($"visits"))
      .withColumn("host", lower(callUDF("parse_url", $"visit.url", lit("HOST"))))
      .withColumn("domain", regexp_replace($"host", "www.", ""))
      .drop("visits")

    val df2 = df.groupBy("uid", "gender_age").agg(collect_list("domain") as "domain_features")
      .select("uid", "gender_age", "domain_features")


    val cv = new CountVectorizer()
      .setInputCol("domain_features")
      .setOutputCol("features")

    val indexer = new StringIndexer()
      .setInputCol("gender_age")
      .setOutputCol("label")

    val labels = indexer.fit(df2).labels

    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.001)

    val lbl = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("prediction_gender_age")
      .setLabels(labels)

    val pipeline = new Pipeline().setStages(Array(cv, indexer, lr, lbl))
    val model = pipeline.fit(df2)

    model.write.overwrite().save(OutputPath + "/model")

    spark.stop()

  }
}
