import scala.sys.process._

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

lazy val root = (project in file("."))
  .settings(name := "kcl-scala-example")

val circeVersion = "0.14.5"
libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.12.512",
  "com.amazonaws" % "amazon-kinesis-client" % "1.15.0",
  "com.amazonaws" % "dynamodb-streams-kinesis-adapter" % "1.6.0",
  "ch.qos.logback" % "logback-classic" % "1.4.8",
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion
)

lazy val setupLocal = taskKey[Unit]("DynamoDB Localを起動し、テーブルを作成する")
setupLocal := {
  "docker compose -p kcl-scala-example -f local/compose.yml up -d".!
  println("running DynamoDB Admin on http://localhost:49001")
}

lazy val destroyLocal = taskKey[Unit]("DynamoDB Localを終了する")
destroyLocal := {
  "docker compose -p kcl-scala-example  -f local/compose.yml down".!
}
