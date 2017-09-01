name := "Simple Project"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq ( 
	"org.apache.spark" %% "spark-core" % "2.1.1",
	"org.apache.spark" %% "spark-sql" % "2.1.1",
	"com.typesafe.scala-logging" %% "scala-logging" % "3.7.1",
	"com.amazon.emr" % "emr-dynamodb-connector" % "4.2.0",
	"com.amazon.emr" % "emr-dynamodb-hadoop" % "4.2.0",
        "org.apache.spark" % "spark-hive_2.11" % "2.2.0"
)
