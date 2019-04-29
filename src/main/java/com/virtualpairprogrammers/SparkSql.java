package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSql {

	public static void main(String[] args) {
	Logger.getLogger("org.apache").setLevel(Level.WARN);
		
//		SparkConf conf = new SparkConf().setAppName("reading from a file").setMaster("local[*]");
//		
//		JavaSparkContext sc = new JavaSparkContext(conf);
	
	SparkSession sparksession = SparkSession.builder()
			.appName("SparkSQL").master("local[*]")
			.config("spark.config.warehouse.dir","file:///tmp/")
			.getOrCreate();
	
	Dataset<Row> dataset = sparksession
			.read()
			.option("header",true)
			.csv("src/main/resources/exams/students.csv");
	
//	dataset.show();
	long numberofRows = dataset.count();
//	System.out.println("The total number of rows are "+ dataset.count());

//	
//	Row firstRow = dataset.first();
//	// To get the second value of the first row and pass the value of the column
//	String subject = firstRow.get(2).toString();
//	// Also can be done with getAs
//	String subjectAs = firstRow.getAs("subject").toString();
//	System.out.println(subject);
	
	Dataset<Row> modernArtResult = dataset.filter("subject = 'Modern Art' AND year>=2007");
	modernArtResult.show();	
	
	sparksession.close();
	
	}

}
