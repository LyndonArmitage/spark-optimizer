name := "spark-optimizer"
version := "0.0.1"
scalaVersion := "2.12.13"

val slf4jVersion = "1.7.32"
val sparkVersion = "3.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql"  % sparkVersion,
  //test-only
  "org.scalatest" %% "scalatest" % "3.2.10" % Test,
  "com.github.mrpowers" %% "spark-fast-tests" % "1.0.0" % Test,
  // testing lineage
  "io.openlineage" % "openlineage-java" % "0.3.1",
  "com.softwaremill.sttp.client3" %% "core" % "3.3.16"
).map(_.exclude("org.slf4j", "slf4j-log4j12"))

libraryDependencies ++= Seq(
  //logging
  "org.slf4j"      % "slf4j-api"       % slf4jVersion,
  "ch.qos.logback" % "logback-classic" % "1.2.6"
)

//val circeVersion = "0.14.1"
//libraryDependencies ++= Seq(
//  "io.circe" %% "circe-core",
//  "io.circe" %% "circe-generic",
//  "io.circe" %% "circe-parser"
//).map(_ % circeVersion)
//
//libraryDependencies += "io.circe" %% "circe-jackson212" % "0.14.0"

scalacOptions ++= Seq(
  "-Xfatal-warnings"
)
