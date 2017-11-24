name := "StackOverflowClustering"
version := "0.1"
scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.2.0",
    "org.apache.spark" %% "spark-sql" % "2.2.0",
    "org.scala-lang.modules" %% "scala-xml" % "1.0.2"
)

// libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
