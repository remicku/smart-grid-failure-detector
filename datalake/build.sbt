ThisBuild / scalaVersion := "2.13.14"
ThisBuild / organization := "smartgrid"
ThisBuild / version      := "0.1.0-SNAPSHOT"

name := "datalake"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.1"

Compile / mainClass := Some("smartgrid.datalake.Main")
Compile / run / fork := true
Compile / run / javaOptions ++= Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
)
