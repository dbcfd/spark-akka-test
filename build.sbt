val akkaVersion = "[2.5.8, 2.5.999]"
val sparkVersion = "[2.2, 2.3)"
val scalaTestVersion = "3.0.+"
val mockitoVersion = "2.8.+"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "sparkka",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
      "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test,
      "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion % Runtime
    ),
    parallelExecution in test := false,
    fork in Test := true
  )
