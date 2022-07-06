name := "Pars"

version := "0.1"

scalaVersion := "2.12.14"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.3" // % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.3" // % "provided"

libraryDependencies+="org.postgresql" % "postgresql" % "42.2.23"