package com.virtualpairprogrammers.streaming;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

public class KafkaViewingPackage {

	public static void main(String[] args) throws InterruptedException {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
		SparkConf conf = new SparkConf().setAppName("Kafka Streaming").setMaster("local[*]");
		JavaStreamingContext sc = new JavaStreamingContext(conf,Durations.seconds(10));
		// Creating topics as collections to pass to consumer props
		Collection<String> topics =Arrays.asList("test");
		Map<String, Object> params =new HashMap<>();
		params.put("bootstrap.servers", "localhost:9092");
		params.put("key.deserializer", StringDeserializer.class);
		params.put("value.deserializer", StringDeserializer.class);
		params.put("group.id","spark-group");
		params.put("auto.offset.reset","latest");
//		
//		 JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(sc,
//				                       LocationStrategies.PreferConsistent(),
//				 					   ConsumerStrategies.Subscribe(topics, params));
		 
		 
		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(sc,  LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics, params));
//		JavaDStream<String> reading = stream.map(value-> value.value());
		JavaPairDStream<String, Long> mapToPair = stream.mapToPair(value-> new Tuple2<>(value.value(),5L));
		JavaPairDStream<String, Long> reduceByKey = mapToPair.reduceByKeyAndWindow((value1,value2)-> value1+value2, Durations.minutes(2));
		// Swapp the rdd value by using swap funtion
		JavaPairDStream<Long, String> swapped = reduceByKey.mapToPair(value-> value.swap());
		// Using transform in javaPairDstream
		JavaPairDStream<Long, String> transformToPair = swapped.transformToPair(rdd-> rdd.sortByKey(false));
		
		transformToPair.print();
		 
		 sc.start();
		 sc.awaitTermination();
		
		
		
		
	}

}
