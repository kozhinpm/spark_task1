ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-sql" % "3.2.1" % "provided").cross(CrossVersion.for3Use2_13)
)
// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.2.1" % "provided"

// https://mvnrepository.com/artifact/com.typesafe.play/play-json
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.10.0-RC6"

// https://mvnrepository.com/artifact/com.fasterxml.jackson.module/jackson-module-scala
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.6"


lazy val root = (project in file("."))
  .settings(
    name := "ScalaLearn"
  )
