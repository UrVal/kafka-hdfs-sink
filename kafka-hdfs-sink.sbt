
// compile with sbt spDist
name := "kafka-hdfs-sink"

version := "1.0"

scalaVersion := "2.10.5"


resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"


libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.5.0"
libraryDependencies += "com.databricks" %% "spark-avro" % "2.0.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0"

