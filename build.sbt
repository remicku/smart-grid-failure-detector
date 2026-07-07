ThisBuild / scalaVersion := "2.13.14"
ThisBuild / organization := "smartgrid"
ThisBuild / version      := "0.1.0-SNAPSHOT"

val fs2KafkaV   = "3.5.1"
val circeV      = "0.14.9"
val catsEffectV = "3.5.4"
val munitV      = "1.0.0"
val jakartaMailV = "2.0.1"
val slf4jSimpleV = "1.7.36"
val postgresqlV = "42.7.3"
val sparkV = "3.5.1"
val hadoopAwsV = "3.3.4"
val awsSdkV = "2.25.70"

lazy val sparkRunSettings = Seq(
  Compile / run / fork := true,
  Compile / run / javaOptions ++= Seq(
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
  )
)

lazy val shared = project
  .in(file("shared"))
  .settings(
    name := "shared",
    libraryDependencies ++= Seq(
      "io.circe" %% "circe-core"    % circeV,
      "io.circe" %% "circe-generic" % circeV,
      "io.circe" %% "circe-parser"  % circeV
    )
  )

lazy val alertDetector = project
  .in(file("alert-detector"))
  .dependsOn(shared)
  .settings(
    name := "alert-detector",
    libraryDependencies ++= Seq(
      "com.github.fd4s" %% "fs2-kafka"   % fs2KafkaV,
      "org.typelevel"   %% "cats-effect" % catsEffectV,
      "org.slf4j"        % "slf4j-simple" % slf4jSimpleV,
      "org.scalameta"   %% "munit"       % munitV % Test
    ),
    testFrameworks += new TestFramework("munit.Framework")
  )

lazy val simulator = project
  .in(file("sensor-simulator"))
  .dependsOn(shared)
  .settings(
    name := "simulator",
    libraryDependencies ++= Seq(
      "com.github.fd4s" %% "fs2-kafka"   % fs2KafkaV,
      "org.typelevel"   %% "cats-effect" % catsEffectV,
      "org.slf4j"        % "slf4j-simple" % slf4jSimpleV
    )
  )

lazy val alertHandler = project
  .in(file("alert-handler"))
  .dependsOn(shared)
  .settings(
    name := "alert-handler",
    libraryDependencies ++= Seq(
      "com.github.fd4s" %% "fs2-kafka"   % fs2KafkaV,
      "org.typelevel"   %% "cats-effect" % catsEffectV,
      "org.postgresql"   % "postgresql"   % postgresqlV,
      "com.sun.mail"     % "jakarta.mail" % jakartaMailV,
      "org.slf4j"        % "slf4j-simple" % slf4jSimpleV,
      "org.scalameta"   %% "munit"       % munitV % Test
    ),
    Compile / mainClass := Some("smartgrid.alerthandler.Main"),
    Compile / run / fork := true,
    testFrameworks += new TestFramework("munit.Framework")
  )

lazy val bronzeIngestor = project
  .in(file("bronze-ingestor"))
  .settings(
    name := "bronze-ingestor",
    libraryDependencies ++= Seq(
      "com.github.fd4s" %% "fs2-kafka"   % fs2KafkaV,
      "org.typelevel"   %% "cats-effect" % catsEffectV,
      "org.slf4j"        % "slf4j-simple" % slf4jSimpleV,
      "software.amazon.awssdk" % "s3"     % awsSdkV
    )
  )

lazy val datalake = project
  .in(file("datalake"))
  .dependsOn(shared)
  .settings(
    name := "datalake",
    Compile / mainClass := Some("smartgrid.datalake.Main"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkV,
      "org.apache.hadoop" % "hadoop-aws" % hadoopAwsV
    )
  )
  .settings(sparkRunSettings)

lazy val analytics = project
  .in(file("analytics"))
  .settings(
    name := "analytics",
    Compile / mainClass := Some("smartgrid.analytics.Main"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkV,
      "org.apache.hadoop" % "hadoop-aws" % hadoopAwsV
    )
  )
  .settings(sparkRunSettings)

lazy val root = project
  .in(file("."))
  .aggregate(shared, alertDetector, simulator, alertHandler, bronzeIngestor, datalake, analytics)
  .settings(name := "smart-grid")
