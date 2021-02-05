name := "spark-optimizer"
version := "0.0.1"
scalaVersion := "2.12.13"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.1",
  "org.apache.spark" %% "spark-sql" % "3.0.1",
  //logging
  "org.slf4j"            % "slf4j-api"                % "1.7.30",
  "ch.qos.logback"       % "logback-classic"          % "1.2.3",
  //test-only
  "org.scalatest" %% "scalatest" % "3.1.1"   % Test,
)

scalacOptions ++= Seq(
  "-Xfatal-warnings"
)

coverageEnabled := true

