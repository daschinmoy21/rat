ThisBuild / organization := "rat"
ThisBuild / scalaVersion := "2.13.14"
ThisBuild / version := "0.1.0-SNAPSHOT"

val sparkV = "3.5.3"

lazy val sparkJob = (project in file("."))
  .settings(
    name := "rat-spark",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkV,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkV,
      "org.apache.spark" %% "spark-hive" % sparkV,
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),
    javacOptions ++= Seq("--release", "17"),
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked"),
    run / fork := true,
    run / javaOptions ++= Seq(
      // the full JDK17 set Spark 3.5 recommends — the Hive/Hadoop clients
      // reach into java.net, java.util and friends via reflection
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      // HDFS data path: connect to datanodes via their advertised hostname
      "-Dspark.hadoop.dfs.client.use.datanode.hostname=true"
    )
  )
