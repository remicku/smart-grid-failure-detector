ThisBuild / scalaVersion := "2.13.14"
ThisBuild / organization := "smartgrid"
ThisBuild / version      := "0.1.0-SNAPSHOT"

Compile / unmanagedSourceDirectories += baseDirectory.value / "../shared/src/main/scala"

name := "alert-detector"

libraryDependencies ++= Seq(
  "com.github.fd4s" %% "fs2-kafka"   % "3.5.1",
  "org.typelevel"   %% "cats-effect" % "3.5.4",
  "org.slf4j"        % "slf4j-simple" % "1.7.36",
  "io.circe"        %% "circe-core"    % "0.14.9",
  "io.circe"        %% "circe-generic" % "0.14.9",
  "io.circe"        %% "circe-parser"  % "0.14.9",
  "org.scalameta"   %% "munit"         % "1.0.0" % Test
)

Compile / mainClass := Some("smartgrid.alertdetector.Main")
testFrameworks += new TestFramework("munit.Framework")
