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

object KafkaHDFSSink{

  def main(args: Array[String]): Unit = {
   if (args.length != 5) {
      System.err.println(s"""
        |Usage: KafkaHDFSSink <brokers> <topics> <destination-url> <offset_to_start_from> <outputformat>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is the topic to read from
        |  <destination-url> is the url prefix (eg:in hdfs) into which to save the fragments. Fragment names will be suffixed with the timestamp. The fragments are directories.(eg: hdfs:///temp/kafka_files/)  
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
	  //.set("spark.driver.allowMultipleContexts", "true")

     val Array(brokers, topics, destinationUrl, offset, outputformat) = args


    val sparkConf = new SparkConf().setAppName("KafkaConsumer_"+topics)
    val sc = new SparkContext(sparkConf)
	val ssc = new StreamingContext(sc, Seconds(2))
	//SparkSQL
	val sqlContext = new SQLContext(sc)
	
	val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> offset)

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    	  messages.foreachRDD( rdd =>{
			  if(!rdd.partitions.isEmpty)
			  {	
		 		  val timestamp: Long = System.currentTimeMillis / 1000
				  if(outputformat == "parquet") {
					  try {
				  		  val json_rdd =  sqlContext.jsonRDD(rdd.map(_._2))
				  		  val df = json_rdd.toDF()
				  		  df.write.mode("append").parquet(destinationUrl+topics)
			  				} catch {
							case NonFatal(t) => println("Waiting for more data")
							}
					}
			  	if(outputformat == "avro") {
				  	try {
			  			val json_rdd =  sqlContext.jsonRDD(rdd.map(_._2))
			  	  		val df = json_rdd.toDF()
			  	  		df.write.mode("append").avro(destinationUrl+topics)
		  	  			} catch {
						case NonFatal(t) => println("Waiting for more data")
						}
					}
				if(outputformat == "json") {
  				  	rdd.map(_._2).saveAsTextFile(destinationUrl+topics+"/clickstream-"+timestamp+".json")
					}
		}
    })

    ssc.checkpoint("hdfs:///"+topics+"__checkpoint")

    ssc.start()
    ssc.awaitTermination()
  }

}
