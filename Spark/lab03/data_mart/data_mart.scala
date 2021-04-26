%AddJar file:/data/home/vladimir.dyukarev/spark-cassandra-connector_2.11-2.4.0.jar
%AddJar file:/data/home/vladimir.dyukarev/postgresql-42.2.13.jar
%AddJar file:/data/home/vladimir.dyukarev/elasticsearch-spark-20_2.11-7.6.2.jar
%AddJar file:/data/home/vladimir.dyukarev/circe-core_2.11-0.11.2.jar

class data_mart {


  spark.conf.set("spark.cassandra.connection.host", "10.0.1.9")
  spark.conf.set("spark.cassandra.connection.port", "9042")
  spark.conf.set("spark.cassandra.output.consistency.level", "ANY")
  spark.conf.set("spark.cassandra.input.consistency.level", "ONE")


  val myTable = Map("table" -> "clients","keyspace" -> "labdata")
  val dfClients = spark
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(myTable)
    .load()

  import org.apache.spark.sql.functions._

  val esOptions =
    Map(
      "es.nodes" -> "10.0.1.9:9200",
      "es.batch.write.refresh" -> "false",
      "es.nodes.wan.only" -> "true"
    )
  val visits = spark.read.format("es").options(esOptions).load("visits")

  val logs = spark.read.json("hdfs:///labs/laba03/weblogs.json")

  val jdbcUrl = "jdbc:postgresql://10.0.1.9:5432/labdata?user=vladimir_dyukarev&password=A3KQleIJ"

  val catg = spark
    .read
    .format("jdbc")
    .option("url", jdbcUrl)
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", "domain_cats")
    .load()

  val joinVisits = visits.join(dfClients, Seq("uid"), "inner")
    .filter('event_type === "view")
    .select('uid, 'category)

  val udfvists = udf((x: String) => "shop_".concat(
    x.toLowerCase()
      .replace(' ','_')
      .replace('-','_')))

  val visits2 = joinVisits.withColumn("category2", udfvists(col("category"))).withColumn("1", lit(1))
  val visits3 = visits2.groupBy("uid").pivot("category2").sum("1").na.fill(0)

  val log_1 = logs.join(dfClients, Seq("uid"), "inner")
    .select('uid, 'visits)
  val log_2  = log_1.select($"uid",explode($"visits")).select("uid", "col.url")

  val log_3 = log_2.withColumn("host", callUDF("parse_url", $"url", lit("HOST")))
    .withColumn("host2", callUDF("replace",$"host" , lit("www."), lit("")))

  import org.apache.spark.sql.Column
  val joinCondition: Column = col("host") === col("domain")

  val joinSite = log_3.join(catg, joinCondition)

  val udflogs = udf((x: String) => "web_".concat(
    x.toLowerCase()
      .replace(' ','_')
      .replace('-','_')))

  val joinSite2 = joinSite
    .withColumn("category2", udflogs(col("category")))
    .withColumn("1", lit(1))

  val joinSite3 = joinSite2.groupBy("uid").pivot("category2").sum("1").na.fill(0)

  // Соединяем все в одну витрину
  val all_final = dfClients.join(visits3, Seq("uid"), "left")
    .join(joinSite3, Seq("uid"), "left")
}
