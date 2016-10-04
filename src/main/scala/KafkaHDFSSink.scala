import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import com.databricks.spark.csv._
import com.databricks.spark.avro._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import scala.util.control.NonFatal
import scala.util.parsing.json._
import org.apache.spark.sql.functions._

object KafkaHDFSSink{

  def main(args: Array[String]): Unit = {
   if (args.length != 5) {
      System.err.println(s"""
        |Usage: KafkaHDFSSink <brokers> <topics> <destination-url> <offset_to_start_from> <outputformat>
        |This consumer assumes data is send in JSON format, if data is not send in readable JSON, it will write the data as text files in hdfs:///malformed_kafka_data/<timestamp>
        |  <brokers> is a list of one or more Kafka brokers 
        |  <topic> this is a generic topic to read from
        |  <destination-url> is the url prefix (eg:in hdfs) into which to save the fragments. Path will be suffixed with @table_name and @schema_version (eg: hdfs:///temp/kafka_files/)  
        |  <offset_to_start_from> is the position from where the comsumer should start to receive data. Choose between: smallest and largest
		|  <output_format> is the file format to output the files to. Choose between: parquet, avro and json.
		""".stripMargin)
      System.exit(1)
    }
   
        //Create SparkContext
    val conf = new SparkConf()
      .setMaster("yarn-client")
      .setAppName("KafkaConsumer")
      .set("spark.executor.memory", "5g")
      .set("spark.rdd.compress","true")
      .set("spark.storage.memoryFraction", "1")
      .set("spark.streaming.unpersist", "true")
	  
     val Array(brokers, topic, destinationUrl, offset, outputformat) = args


    val sparkConf = new SparkConf().setAppName("KafkaConsumer_Generic")
    val sc = new SparkContext(sparkConf)
	val ssc = new StreamingContext(sc, Seconds(2))
	//SparkSQL
	val sqlContext = new SQLContext(sc)

	val topicSet = topic.split(",").toSet
	
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> offset)

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicSet)

    	  messages.foreachRDD( rdd =>{
			  if(!rdd.partitions.isEmpty)
			  {	
		 		  val timestamp: Long = System.currentTimeMillis / 1000
				  if(outputformat == "parquet") {
					  	//val test_json = JSON.parseFull(rdd.map(_._2).toString())
					  	//test_json match {
  							//case None => rdd.map(_._2).saveAsTextFile(destinationUrl+"malformed_json_data/data-"+timestamp+".txt")
  							//case Some(e) => 
  							try {
					  	  						val json_rdd = sqlContext.jsonRDD(rdd.map(_._2))
				  		  						val df = json_rdd.toDF()
				  		  
				  		  						//define table as String
				  		  						val table_name_array = df.select("@table").limit(1).collect()
				  		  						val table_name_string = table_name_array(0).toString().stripPrefix("[").stripSuffix("]").trim
				  		  						//define schema_version as String
				  		  						val schema_version_array = df.select("@schema_version").limit(1).collect()
				  		  						val schema_version_string = schema_version_array(0).toString().stripPrefix("[").stripSuffix("]").trim
				  		  						//define p_key as String
				  		  						val primarykey_array = df.select("@p_key").limit(1).collect()
				  		  						val primarykey_string = primarykey_array(0).toString().stripPrefix("[").stripSuffix("]").trim
				  		  						//define updated as String
				  		  						val updated_array = df.select("@update").limit(1).collect()
				  		  						val updated_string = updated_array(0).toString().stripPrefix("[").stripSuffix("]").trim

				  		  						//if record is an update, upload latest timestamp in small table
				  		  						if (updated_string == "1") {
				  		  							df.select(primarykey_string, "@timestamp").write.mode("append").parquet(destinationUrl+table_name_string+"/updated_records")
				  		  							val updated_timestamp = sqlContext.read.parquet(destinationUrl+table_name_string+"/updated_records").groupBy(primarykey_string).agg(max("@timestamp") as "@timestamp")
				  		  							updated_timestamp.write.mode("overwrite").parquet(destinationUrl+table_name_string+"/updated_records")
				  		  						} else {
				  		  							df.select(primarykey_string, "@timestamp").write.mode("append").parquet(destinationUrl+table_name_string+"/updated_records")
				  		  						}

				  		  						//write main Dataframe to parquet
				  		  						df.write.mode("append").parquet(destinationUrl+table_name_string+"/schema-version-"+schema_version_string)

			  									} catch {
														case NonFatal(t) => rdd.map(_._2).saveAsTextFile(destinationUrl+"unknown_schema/data-"+timestamp+".json")
														//sqlContext.jsonRDD(rdd.map(_._2)).toDF().write.mode("append").parquet(destinationUrl+"malformed_schema_parquet/data-"+timestamp)
														}
										}	
					//}


					
			 	if(outputformat == "avro") {
				  	try {
			  			val json_rdd =  sqlContext.jsonRDD(rdd.map(_._2))
			  	  		val df = json_rdd.toDF()
			  	  		df.write.mode("append").avro(destinationUrl+topic)
		  	  			} catch {
						case NonFatal(t) => println("Waiting for more data")
						}
					}
				if(outputformat == "json") {
  				  	rdd.map(_._2).saveAsTextFile(destinationUrl+topic+"/clickstream-"+timestamp+".json")
					}
		}
    })

    ssc.checkpoint("hdfs:///generic_topic__checkpoint")

    ssc.start()
    ssc.awaitTermination()
  }

}
