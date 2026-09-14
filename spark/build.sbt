ThisBuild / organization := "rat"
ThisBuild / scalaVersion := "2.13.14"
ThisBuild / version := "0.1.0-SNAPSHOT"

val sparkV = "3.5.3"

lazy val sparkJob = (project in file("."))
  .settings(
    name := "rat-spark",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkV,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkV
    ),
    javacOptions ++= Seq("--release", "17"),
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked"),
    run / fork := true,
    run / javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    )
  )
