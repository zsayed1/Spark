package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Iterables;

import scala.Tuple2;

public class OptimizedPairRDD {
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		List<String> input = new ArrayList<>();
		input.add("WARN: Tuesday 2 March 2019");
		input.add("ERROR: Tuesday 3 Jab 2018");
		input.add("FATAL: Tuesday 4 May 2017");
		input.add("WARN: Tuesday 5 Feb 2016");
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		// Set App Name stating the Name of the app and Set master with wildcard stating the url of master
		SparkConf conf = new SparkConf().setAppName("Starting Spark").setMaster("local[*]");
		// Passn the conf to JavaSparkContext to read it
		JavaSparkContext sc = new JavaSparkContext(conf);
		// Pass the input to intialize RDD
		sc.parallelize(input)
		.mapToPair(rawValue -> new Tuple2<String, Long>(rawValue.split(":")[0], 1L))
		.reduceByKey((value1, value2)-> value1+ value2)
		.foreach(tuple -> System.out.println(tuple._1 + "\t has "+ tuple._2 + "\t Instances"));
		
		
////		GroupBy
//		
//		sc.parallelize(input)
//		.mapToPair(rawValue -> new Tuple2<String, Long>(rawValue.split(":")[0], 1L))
//		.groupByKey()
//		.foreach(tuple -> System.out.println(tuple._1 +"  has " + Iterables.size(tuple._2)+ "  instances"));
//		
		sc.close();

	}

}
