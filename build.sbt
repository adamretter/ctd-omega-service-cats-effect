ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "ctd-omega-service-cats-effect",
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % "3.3.14",
      "org.typelevel" %% "log4cats-slf4j" % "2.5.0",
      "dev.fpinbo" %% "jms4s-active-mq-artemis" % "0.2.1",   // TODO(AR) may need to create a provider for SQS

      "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.19.0" % Runtime
    )
  )
