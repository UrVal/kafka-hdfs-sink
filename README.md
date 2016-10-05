# Kafka Consumer - Using Spark Streaming

Simple serialization of data from  Kafka to HDFS. Uses direct a direct connection to the brokers instead of zookeeper based tracking.

This branch assumes data in a generic topic comes from different database tables. It parses the data from the topic as JSON and looks for:

"@table" -> Name of the table the data originates from. It's used to suffix the HDFS destination path with the table name.
"@schema_version" -> This accounts for schema changes in the originating database, that might happen over time.
"@p_key" -> primary key of the originating table
"@update" -> it's a 0/1 flag that takes into account if data is an update
"@timestamp" -> timestamp when the record was written in Kafka Topic

For data that doesn't have this structure it will failback to write an unknown schema JSON file (hdfs:///destinationUrl+"unknown_schema/data-"+timestamp+".json")

Consumer can write in Parquet, Avro and JSON format, by appending data to the existing files.

It takes the following input parameters when lunching the Spark Job:

<code>brokers</code> is a list of one or more Kafka brokers 
<code>topic</code> this is a generic topic to read from
<code>destination-url</code> is the url prefix (eg:in hdfs) into which to save the fragments. Path will be suffixed with @table_name and @schema_version (eg: hdfs:///temp/kafka_files/)  
<code>offset_to_start_from</code> is the position from where the comsumer should start to receive data. Choose between: smallest and largest
<code>output_format</code> is the file format to output the files to. Choose between: parquet, avro and json.

In order to cope with the updated records the consumer also outputs a table with the primary key and latest timestamp. This can be used for a compaction batch job that can drop the old records from the main data in HDFS. 

To compile use:
<code>
sbt package
</code>

To submit this job use:
<code>
spark-submit --name KafkaHDFSSink --class KafkaHDFSSink  --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.1 /tmp/kafka-consumer-serializer_2.10-1.0.jar BROKER_FQDN_1:9092,BROKER_FQDN_2:9092,BROKER_FQDN_N:9092 TOPIC_NAME <HDFS:///path/to/write/files> smallest parquet
</code>
