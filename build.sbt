name := "akka-amqp"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.10.3"

parallelExecution in Test := false

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.2.3"

libraryDependencies += "com.rabbitmq" % "amqp-client" % "3.2.2"

libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.2.3" % "test"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.0" % "test"
