package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class FlatMaps {

	public static void main(String[] args) {
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
//		JavaRDD<String> sentence = sc.parallelize(input);
//		
//		// flat maps requires Arraylist because it needs to convert the Array in to list 
//		JavaRDD<String> words = sentence.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
//		// Here we are specifying that if the length of the word is more that 1, only then add it to the new RDD created
//		JavaRDD filteredwords= words.filter(word -> word.length() > 1);
//		filteredwords.foreach(value -> System.out.println(value)); 
		
		
//		Optimized

		sc.parallelize(input)
				.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
				.filter(word -> word.length() > 1)
				.foreach(value -> System.out.println(value));
				
		sc.close();
	
	}

}
