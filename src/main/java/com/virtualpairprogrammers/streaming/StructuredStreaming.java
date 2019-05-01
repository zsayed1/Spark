package com.virtualpairprogrammers.streaming;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class StructuredStreaming {

	public static void main(String[] args) throws StreamingQueryException {

		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
		SparkSession spark = SparkSession.builder()
										 .appName("Structured Streaming")
										 .master("local[*]")
										 .getOrCreate();
		// This is how we subscribe to a topic in spar kstructured streaming with sql
//		Dataset<Row> dataset= spark.readStream()
//								   .format("kafka")
//								   .option("kafka.bootstrap.servers","localhost:9092")
//								   .option("subscribe","test")
//								   .load();
		
		spark.conf().set("spark.sql.shuffle.partitions","10");
								   
		
		Dataset<Row> dataset = spark.readStream()
				.format("kafka")
				.option("kafka.bootstrap.servers", "localhost:9092")
				.option("subscribe", "test")
				//.option("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
				.load();
		// @formatter:on

		// Using the sql statments
		dataset.createOrReplaceTempView("test_table");
		Dataset<Row> results = spark.sql("select window, CAST(value AS STRING) AS course, sum(5) AS seconds from test_table GROUP BY window(timestamp,'2 minutes'), course ORDER BY seconds DESC");
		
		StreamingQuery query = results.writeStream()
			   .format("console")
			   .outputMode(OutputMode.Complete())	
			   .option("truncate", false)
			   .option("numRows",50)
			   .start();
		query.awaitTermination();
		
	}

}
