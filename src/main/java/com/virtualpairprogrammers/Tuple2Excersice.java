package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Tuple2Excersice {
	public static void main(String[] args) {
		// TODO Auto-generated method stub\
		List<Integer> input = new ArrayList<>();
		input.add(9);
		input.add(16);
		input.add(25);
		input.add(36);
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		// Set App Name stating the Name of the app and Set master with wildcard stating the url of master
		SparkConf conf = new SparkConf().setAppName("Starting Spark").setMaster("local[*]");
		// Passn the conf to JavaSparkContext to read it
		JavaSparkContext sc = new JavaSparkContext(conf);
		// Pass the input to intialize RDD 
		JavaRDD<Integer> myrdd = sc.parallelize(input);
//		Creating tuple using Tuple2 class
		JavaRDD<Tuple2<Integer,Double>> tupleRDD = myrdd.map(value -> new Tuple2<Integer, Double>(value,Math.sqrt(value)));
}
}