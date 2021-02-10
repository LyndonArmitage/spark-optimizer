name := "spark-optimizer"
version := "0.0.1"
scalaVersion := "2.12.13"

val slf4jVersion = "1.7.30"
val sparkVersion = "3.0.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql"  % sparkVersion,
  //test-only
  "org.scalatest" %% "scalatest" % "3.1.1" % Test
).map(_.exclude("org.slf4j", "slf4j-log4j12"))

libraryDependencies ++= Seq(
  //logging
  "org.slf4j"      % "slf4j-api"       % slf4jVersion,
  "ch.qos.logback" % "logback-classic" % "1.2.3"
)

scalacOptions ++= Seq(
  "-Xfatal-warnings"
)
