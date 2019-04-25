package com.virtualpairprogrammers;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class ReadFromAFile {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("reading from a file").setMaster("local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> initialRDD = sc.textFile("src/main/resources/subtitles/input.txt");
		
//		initialRDD
//			.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
//			.filter(word -> word.length()>1)
//			.foreach(value-> System.out.println(value));
	
		//JUST TO TEST
		//We take the initial RDD with .take(n) fucntion
		// Save it to a JAVA Collectipn List as it spits JAVA Collection out
//		List<String> take = initialRDD.take(50);
		// Print the RDD with for each 
//		take.forEach(value -> System.out.println(value));
		
		
		// Removing the Anything which is not a word
		JavaRDD<String> lettersOnlyRDD = initialRDD
				.map(sentence -> sentence.replaceAll("[^a-zA-Z ]*", ""));
	
		
		// Removing spaces from the rdd
		JavaRDD<String> removeBlankRDD = lettersOnlyRDD
				.map(sentence -> sentence.replaceAll("^\\s", ""));
		
		// Removing words which does not contain more than one character and trimming it too
		JavaRDD<String> filteredremoveBlankRDD = removeBlankRDD
				.filter(value->value.trim().length()>1);
		
//		filteredremoveBlankRDD.foreach(value-> System.out.println(value));
		// Spliting the sentence and saving it to new RDD with String.split function
		JavaRDD<String> filteredremoveBlankFlatRDD =  filteredremoveBlankRDD.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());
		
		// Removing words which are boring 
		JavaRDD<String> notBoringWords= filteredremoveBlankFlatRDD
		.filter(word -> Util.isNotBoring(word));
		
		 
//		notBoringWords.foreach(value -> System.out.println(value));		
		
		// Mapping to pairRDD to calculate the count
		JavaPairRDD<String, Long> conclusionToPair = notBoringWords.mapToPair(value -> new Tuple2<String, Long>(value, 1L));
		
		// Calculating count and saving it to new RDD
		JavaPairRDD<String, Long> conclusion = conclusionToPair.reduceByKey((value1, value2) -> value1 + value2);
		
		// Switching because ByKey fucntions can be used		
		JavaPairRDD<Long, String> switched = conclusion.mapToPair(tuple -> new Tuple2<Long,String>(tuple._2,tuple._1));
		
		// Using sortByKey fucntion
		// sortByKey as false will set it to greater to lower values
		JavaPairRDD<Long, String> sorted=switched.sortByKey(false);
		
		sorted.foreach(tuple -> System.out.println(tuple));
		
		
		sc.close();
	}
	
	

}
