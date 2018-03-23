organization := "sbgfeedme"

name := "consumer"

version := "0.1"

scalaVersion := "2.12.4"

mainClass in Compile := Some("sbgfeedme.consumer.FeedConsumer")

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.5.9",
  "com.typesafe.akka" %% "akka-stream-testkit" % "2.5.9" % Test,
  "org.json4s" %% "json4s-native" % "3.2.11",
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.2.0",
  "org.scalactic" %% "scalactic" % "3.0.4",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test"
)

resolvers ++= Seq(
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
)

enablePlugins(DockerPlugin)
dockerAutoPackageJavaApplication()