package com.virtualpairprogrammers.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class LogStreamingRDD {

	public static void main(String[] args) throws InterruptedException {

		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName("Log Streaming").setMaster("local[*]");
		// Giving the conf and duration when to poll from the context
		JavaStreamingContext sc = new JavaStreamingContext(conf,Durations.seconds(5));
		// Reading from the stream by providing url and port
		JavaReceiverInputDStream<String> socketTextStream = sc.socketTextStream("localhost", 8989);
		
		JavaDStream<String> show = socketTextStream.map(value -> value);
	   
		JavaPairDStream<String, Long> withFiltered = show.mapToPair(value-> new Tuple2<>(value.split(",")[0],1L));
		
	    JavaPairDStream<String, Long> reduceByKeyStream = withFiltered.reduceByKeyAndWindow((value1,value2)->value1+value2, Durations.minutes(2));

	    reduceByKeyStream.print();
		sc.start();
		sc.awaitTermination();
	}

}
