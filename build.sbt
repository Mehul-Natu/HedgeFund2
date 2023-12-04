ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.11"

lazy val root = (project in file("."))
  .settings(
    name := "HedgeFund2"
  )
libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.4.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.2"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.2"

// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.3.2"
//libraryDependencies += "com.nrinaudo" %% "kantan.csv" % "0.7.0"
//libraryDependencies += "com.nrinaudo" %% "kantan.csv-generic" % "0.7.0"
