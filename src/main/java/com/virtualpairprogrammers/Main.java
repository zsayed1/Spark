package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Main {

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
		// Need to specify the RDD type
		JavaRDD<Double> sqrtRDD = myrdd.map(value -> Math.sqrt(value));
		// We need to print each value in RDD we do it with foreach() function and then print the value
		sqrtRDD.foreach(value -> System.out.println(value));
//		The above statement can be optimized to 
//		sqrtRDD.foreach(System.out::println); look for rdd.collect();
//		How many elements in RDD
		System.out.println(sqrtRDD.count());
//		But when we have to count the number of values inside the RDD with simple map and reduce
		JavaRDD<Long> singleInteger = sqrtRDD.map(value -> 1L);
		Long count= singleInteger.reduce((value1, value2)-> value1 + value2);
		System.out.println(count);
		
		
//		Creating a tuple in JAVA Using Object in JavaRDD
		JavaRDD<IntergerToSquareRoot> javaObjectRDD = myrdd.map(value -> new IntergerToSquareRoot(value));
		
//		Creating tuple using Tuple2 class
		JavaRDD<Tuple2<Integer,Double>> tupleRDD = myrdd.map(value -> new Tuple2<Integer, Double>(value,Math.sqrt(value)));
		
		
		sc.close();
	}

}
