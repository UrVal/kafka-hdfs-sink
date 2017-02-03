# kafka-hdfs-sink

Simple serialization of data from  Kafka to HDFS. Uses direct a direct connection to the brokers instead of zookeeper based tracking.

Creates partitioned files in HDFS.

See http://spark.apache.org/docs/latest/streaming-kafka-integration.html for more information

To compile use:
<code>
sbt package
</code>

This application takes 6 parameters:

BROKERS: List of Kafka Brokers with port number and splited with comma

DESTINATION URL: Location to save the files. Can be any supported filesystem location: FILE, HDFS, Bigstep Datalake etc.

KAFKA TOPIC OFFSET: Where should the broker start reading events. (smallest or latest)

OUTPUT FORMAT: What format should the data be written into. CHose between: parquet, avro and json.

TIME: Number of seconds between succesive reads from the kafka topic.

To submit this job use:
<code>
/opt/spark-1.6.1-bin-hadoop2.6/bin/spark-submit --name KafkaHDFSSink --class KafkaHDFSSink  --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.1 /tmp/kafka-consumer-serializer_2.10-1.0.jar BROKER_FQDN_1:9092,BROKER_FQDN_2:9092,BROKER_FQDN_N:9092 TOPIC_NAME <HDFS:///path/to/write/files> OFFSET FORMAT TIME
</code>
