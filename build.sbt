ThisBuild / scalaVersion := "2.13.14"
ThisBuild / organization := "smartgrid"
ThisBuild / version      := "0.1.0-SNAPSHOT"

val fs2KafkaV   = "3.5.1"
val circeV      = "0.14.9"
val catsEffectV = "3.5.4"
val munitV      = "1.0.0"

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
      "org.scalameta"   %% "munit"       % munitV % Test
    ),
    testFrameworks += new TestFramework("munit.Framework")
  )

// lazy val simulator = project.in(file("simulator")).dependsOn(shared)
// lazy val alertHandler = project.in(file("alert-handler")).dependsOn(shared)
// lazy val datalake = project.in(file("datalake")).dependsOn(shared)
// lazy val analytics = project.in(file("analytics")).dependsOn(shared)

lazy val root = project
  .in(file("."))
  .aggregate(shared, alertDetector)
  .settings(name := "smart-grid")
