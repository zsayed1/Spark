package com.virtualpairprogrammers;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ReadFromAFile {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("reading from a file").setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> initialRDD = sc.textFile("src/main/resources/subtitles/input.txt");
		
		initialRDD
			.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
			.filter(word -> word.length()>1)
			.foreach(value-> System.out.println(value));
		
		sc.close();
	}
	
	

}
