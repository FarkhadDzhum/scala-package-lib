name := "bigdata-trigger-conf"

version := "1.0"

scalaVersion := "2.11.8"

val kafkaVersion = "2.0.0"
val sparkVersion = "2.4.1"
val kafkaStreamVersion = "2.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion,
  "org.apache.kafka" % "kafka_2.11" % kafkaVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion
)
