ThisBuild / scalaVersion := "2.13.14"
ThisBuild / organization := "smartgrid"
ThisBuild / version      := "0.1.0-SNAPSHOT"

name := "bronze-ingestor"

libraryDependencies ++= Seq(
  "com.github.fd4s" %% "fs2-kafka"   % "3.5.1",
  "org.typelevel"   %% "cats-effect" % "3.5.4",
  "org.slf4j"        % "slf4j-simple" % "1.7.36"
)

Compile / mainClass := Some("smartgrid.bronzeingestor.Main")
