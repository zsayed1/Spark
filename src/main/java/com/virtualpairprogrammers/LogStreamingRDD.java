package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class LogStreamingRDD {

	public static void main(String[] args) throws InterruptedException {

		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName("Log Streaming").setMaster("local[*]");
		// Giving the conf and duration when to poll from the context
		JavaStreamingContext sc = new JavaStreamingContext(conf,Durations.seconds(30));
		// Reading from the stream by providing url and port
		JavaReceiverInputDStream<String> socketTextStream = sc.socketTextStream("localhost", 8989);
		
		JavaDStream<String> show = socketTextStream.map(value -> value);
		
		show.print();
		sc.start();
		sc.awaitTermination();
		
	}

}
