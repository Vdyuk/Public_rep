import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import scala.util.{Try}


object users_items extends App {

  val spark = SparkSession.builder()
    .appName("lab05_dyukarev")
    //.config("spark.master", "local[*]")
    .getOrCreate()

  import spark.implicits._

  val updt = spark.conf.get("spark.users_items.update")
  val outputDir = spark.conf.get("spark.users_items.output_dir")
  val inputDir = spark.conf.get("spark.users_items.input_dir")

  // Читаем
  val view = spark.read.json(s"$inputDir/view")
  val buy = spark.read.json(s"$inputDir/buy")

  // вычисляем последнюю дату для записи
  spark.conf.set("spark.sql.session.timeZone", "UTC")

  val for_dt = view.union(buy)
    .withColumn("date_UTC", from_unixtime(col("timestamp")/1000,"yyyy-MM-dd"))
    .select('category, 'date, 'date_UTC)
    .withColumn("date2", to_date($"date_UTC", "yyyy-MM-dd"))

  val maxdt = for_dt.select(date_format(max("date2"),"yyyyMMdd")).first().get(0)

  // В случае апдейта читаем ранее записанное
  val x = Try(spark.read.parquet(s"$outputDir/*")).toOption

/*  def readOld(): Try[DataFrame] = Try {
    val o = spark.read.parquet(s"$outputDir")
    Success(o)
  }*/


  val visits = if (updt == 1 & x!=None) {
          view.union(buy).union(x.get)
    }
   else view.union(buy)


  // преобразуем item_id
  val udfka = udf((x: String, y: String) =>
    x.concat("_").concat(
      y.toLowerCase()
        .replace(' ','_')
        .replace('-','_')))

  val visits2 = visits
    .withColumn("item_id_2", udfka(col("event_type"),col("item_id")))
    .withColumn("1", lit(1))

  // делаем матрицу
  val visits3 = visits2
    .filter(col("uid").isNotNull)
    .groupBy("uid").pivot("item_id_2")
    .sum("1").na.fill(0)


  // пишем
  visits3
    .write
    .mode("overwrite")
    .parquet(s"$outputDir/$maxdt")


}
