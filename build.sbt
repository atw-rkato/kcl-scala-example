import scala.sys.process._

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

lazy val root = (project in file("."))
  .settings(name := "kcl2-consumer-scala-example")

libraryDependencies ++= Seq(
  "software.amazon.awssdk" % "dynamodb" % "2.20.107",
  "software.amazon.kinesis" % "amazon-kinesis-client" % "2.5.1",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
  "ch.qos.logback" % "logback-classic" % "1.4.8"
)

lazy val setupLocal = taskKey[Unit]("DynamoDB Localを起動し、テーブルを作成する")
setupLocal := {
  "docker compose -p kcl2-consumer-scala-example -f local/compose.yml up -d".!
  "./local/create-table.sh".!
  println("running DynamoDB Admin on http://localhost:49001")
}

lazy val destroyLocal = taskKey[Unit]("DynamoDB Localを終了する")
destroyLocal := {
  "docker compose -p kcl2-consumer-scala-example  -f local/compose.yml down".!
}
