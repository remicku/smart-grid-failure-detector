package smartgrid.datalake

import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import smartgrid.shared.AlertThresholds

object DataLakeJob {

  private val bronzePath = "s3a://smartgrid-lake/bronze/telemetry"
  private val silverPath = "s3a://smartgrid-lake/silver/telemetry"
  private val goldPath = "s3a://smartgrid-lake/gold/telemetry"

  def main(args: Array[String]): Unit = {
    val bronzeInput = args.headOption.getOrElse(bronzePath)
    val silverOutput = args.drop(1).headOption.getOrElse(silverPath)
    val goldOutput = args.drop(2).headOption.getOrElse(goldPath)

    val spark =
      SparkSession
        .builder()
        .appName("smart-grid-datalake")
        .master(env("SPARK_MASTER", "local[*]"))
        .config("spark.hadoop.fs.s3a.endpoint", env("S3_ENDPOINT", "http://localhost:9000"))
        .config("spark.hadoop.fs.s3a.access.key", env("S3_ACCESS_KEY", "smartgrid"))
        .config("spark.hadoop.fs.s3a.secret.key", env("S3_SECRET_KEY", "smartgrid"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", env("S3_SSL_ENABLED", "false"))
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()

    val bronze = spark.read.json(bronzeInput)
    val silver = toSilver(bronze)
    val gold = toGold(silver)

    silver.write.mode(SaveMode.Overwrite).partitionBy("region").parquet(silverOutput)
    gold.write.mode(SaveMode.Overwrite).partitionBy("region").parquet(goldOutput)

    println(s"[datalake] bronze=${bronze.count()}, silver=${silver.count()}, gold=${gold.count()}")
    println(s"[datalake] silver=$silverOutput")
    println(s"[datalake] gold=$goldOutput")

    spark.stop()
  }

  def toSilver(bronze: DataFrame): DataFrame =
    bronze
      .select(
        lower(trim(col("sensorId"))).as("sensorId"),
        lower(trim(col("transformerId"))).as("transformerId"),
        col("timestamp").cast("string").as("timestamp"),
        lower(trim(col("region"))).as("region"),
        col("voltage").cast("double").as("voltage"),
        col("current").cast("double").as("current"),
        col("temperature").cast("double").as("temperature"),
        normalizeLoad(col("load").cast("double")).as("load"),
        col("failureRiskScore").cast("double").as("failureRiskScore"),
        upper(trim(col("status"))).as("status")
      )
      .withColumn("eventTime", parseTimestamp(col("timestamp")))
      .withColumn("isAlert", alertSignal)
      .filter(validSilverRow)

  def toGold(silver: DataFrame): DataFrame =
    silver
      .groupBy(
        col("transformerId"),
        col("region"),
        window(col("eventTime"), "1 hour").as("timeWindow")
      )
      .agg(
        avg(col("voltage")).as("voltage"),
        avg(col("current")).as("current"),
        avg(col("temperature")).as("temperature"),
        avg(col("load")).as("load"),
        avg(col("failureRiskScore")).as("failureRiskScore"),
        max(col("failureRiskScore")).as("maxFailureRiskScore"),
        sum(when(col("isAlert"), lit(1L)).otherwise(lit(0L))).as("alertCount"),
        count(lit(1)).as("measurementCount")
      )
      .select(
        col("transformerId"),
        col("timeWindow.start").as("windowStart"),
        col("timeWindow.end").as("windowEnd"),
        col("region"),
        col("voltage"),
        col("current"),
        col("temperature"),
        col("load"),
        col("failureRiskScore"),
        col("maxFailureRiskScore"),
        col("alertCount"),
        col("measurementCount"),
        when(col("alertCount") > lit(0L), lit("ALERT")).otherwise(lit("NORMAL")).as("status")
      )

  private def normalizeLoad(load: Column): Column =
    when(load > lit(1.0) && load <= lit(100.0), load / lit(100.0)).otherwise(load)

  private def parseTimestamp(timestamp: Column): Column =
    to_timestamp(regexp_replace(regexp_replace(timestamp, "T", " "), "Z", ""))

  private def alertSignal: Column =
    col("failureRiskScore") >= lit(AlertThresholds.WarningThreshold) ||
      col("status").isin("ALERT", "WARNING", "CRITICAL", "OVERLOAD", "OVERHEATING")

  private def validSilverRow: Column =
    col("sensorId").isNotNull &&
      col("transformerId").isNotNull &&
      col("eventTime").isNotNull &&
      col("region").isNotNull &&
      col("voltage").isNotNull &&
      col("current").isNotNull &&
      col("temperature").isNotNull &&
      col("load").between(0.0, 1.0) &&
      col("failureRiskScore").between(0.0, 1.0)

  private def env(name: String, fallback: String): String =
    sys.env.lift(name).filter(_.nonEmpty).getOrElse(fallback)
}
