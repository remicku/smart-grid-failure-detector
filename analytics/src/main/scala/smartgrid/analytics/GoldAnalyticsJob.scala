package smartgrid.analytics

import java.nio.charset.StandardCharsets

import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import scala.util.Using

object GoldAnalyticsJob {

  private val goldPath = "s3a://smartgrid-lake/gold/telemetry"
  private val statsPath = "s3a://smartgrid-lake/stats"

  def main(args: Array[String]): Unit = {
    val goldInput = args.headOption.getOrElse(goldPath)
    val statsOutput = args.drop(1).headOption.getOrElse(statsPath)

    val spark =
      SparkSession
        .builder()
        .appName("smart-grid-analytics")
        .master(env("SPARK_MASTER", "local[*]"))
        .config("spark.hadoop.fs.s3a.endpoint", env("S3_ENDPOINT", "http://localhost:9000"))
        .config("spark.hadoop.fs.s3a.access.key", env("S3_ACCESS_KEY", "smartgrid"))
        .config("spark.hadoop.fs.s3a.secret.key", env("S3_SECRET_KEY", "smartgrid"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", env("S3_SSL_ENABLED", "false"))
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()

    val gold = spark.read.parquet(goldInput)

    val byRegion = regionsHighestRisk(gold)
    val byTransformer = transformersMostAlerts(gold)
    val loadByHour = averageLoadByHour(gold)
    val riskTrend = failureRiskTrend(gold)

    save("regions-highest-risk", byRegion, statsOutput)
    save("transformers-most-alerts", byTransformer, statsOutput)
    save("average-load-by-hour", loadByHour, statsOutput)
    save("failure-risk-trend", riskTrend, statsOutput)

    AnalyticsPage.write(
      spark,
      statsOutput,
      Seq(
        AnalyticsPage.Section("Regions with highest failure risk", byRegion),
        AnalyticsPage.Section("Transformers with most alerts", byTransformer),
        AnalyticsPage.Section("Average load by hour", loadByHour),
        AnalyticsPage.Section("Failure risk trend", riskTrend)
      )
    )

    spark.stop()
  }

  private def env(name: String, fallback: String): String =
    sys.env.lift(name).filter(_.nonEmpty).getOrElse(fallback)

  def regionsHighestRisk(gold: DataFrame): DataFrame =
    gold
      .groupBy(col("region"))
      .agg(
        avg(col("failureRiskScore")).as("averageRisk"),
        max(col("maxFailureRiskScore")).as("highestRisk"),
        sum(col("measurementCount")).as("measurements")
      )
      .orderBy(desc("averageRisk"))

  def transformersMostAlerts(gold: DataFrame): DataFrame =
    gold
      .groupBy(col("transformerId"))
      .agg(
        sum(col("alertCount")).as("alerts"),
        avg(col("failureRiskScore")).as("averageRisk")
      )
      .orderBy(desc("alerts"), desc("averageRisk"))

  def averageLoadByHour(gold: DataFrame): DataFrame =
    gold
      .withColumn("hour", hour(col("windowStart")))
      .groupBy(col("hour"))
      .agg(avg(col("load")).as("averageLoad"))
      .orderBy(col("hour"))

  def failureRiskTrend(gold: DataFrame): DataFrame =
    gold
      .groupBy(col("windowStart"), col("windowEnd"))
      .agg(avg(col("failureRiskScore")).as("averageRisk"))
      .orderBy(col("windowStart"))

  private def save(name: String, data: DataFrame, basePath: String): Unit = {
    println(s"[analytics] $name")
    data.show(truncate = false)
    data.coalesce(1).write.mode(SaveMode.Overwrite).json(s"$basePath/$name")
  }
}

private object AnalyticsPage {

  final case class Section(title: String, data: DataFrame)

  def write(spark: SparkSession, basePath: String, sections: Seq[Section]): Unit = {
    val path = new HadoopPath(basePath, "index.html")
    val fileSystem = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val bytes = page(sections).getBytes(StandardCharsets.UTF_8)
    Using.resource(fileSystem.create(path, true))(_.write(bytes))
    println(s"[analytics] page $path")
  }

  private def page(sections: Seq[Section]): String = {
    val body = sections.map(section).mkString("\n")

    s"""<!doctype html>
       |<html>
       |<head>
       |  <meta charset="utf-8">
       |  <title>Smart Grid Analytics</title>
       |  <style>
       |    body { font-family: Arial, sans-serif; margin: 32px; color: #222; }
       |    h1 { font-size: 24px; margin-bottom: 8px; }
       |    h2 { font-size: 18px; margin-top: 28px; }
       |    table { border-collapse: collapse; width: 100%; max-width: 1000px; }
       |    th, td { border: 1px solid #ddd; padding: 7px 9px; text-align: left; }
       |    th { background: #f2f2f2; }
       |  </style>
       |</head>
       |<body>
       |  <h1>Smart Grid Analytics</h1>
       |$body
       |</body>
       |</html>
       |""".stripMargin
  }

  private def section(section: Section): String = {
    val columns = section.data.columns.toSeq
    val rows = section.data.limit(20).collect().toSeq
    val header = columns.map(name => s"<th>${escape(name)}</th>").mkString
    val body =
      rows
        .map(row => columns.map(name => s"<td>${format(row.getAs[Any](name))}</td>").mkString)
        .map(cells => s"<tr>$cells</tr>")
        .mkString("\n")

    s"""  <section>
       |    <h2>${escape(section.title)}</h2>
       |    <table>
       |      <thead><tr>$header</tr></thead>
       |      <tbody>
       |$body
       |      </tbody>
       |    </table>
       |  </section>""".stripMargin
  }

  private def format(value: Any): String =
    Option(value)
      .map {
        case number: Double => f"$number%.3f"
        case number: Float  => f"${number.toDouble}%.3f"
        case other          => escape(other.toString)
      }
      .getOrElse("")

  private def escape(value: String): String =
    value
      .replace("&", "&amp;")
      .replace("<", "&lt;")
      .replace(">", "&gt;")
      .replace("\"", "&quot;")
}
