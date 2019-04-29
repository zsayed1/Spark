package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLLambda {
	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
			
//			SparkConf conf = new SparkConf().setAppName("reading from a file").setMaster("local[*]");
//			
//			JavaSparkContext sc = new JavaSparkContext(conf);
		
		SparkSession sparksession = SparkSession.builder()
				.appName("SparkSQL").master("local[*]")
				.config("spark.config.warehouse.dir","file:///tmp/")
				.getOrCreate();
		
		Dataset<Row> dataset = sparksession
				.read()
				.option("header",true)
				.csv("src/main/resources/exams/students.csv");
		
		
		// Writing lamda instead of SQL 
		Dataset<Row> moderArtResults = dataset.filter(row-> row.getAs("subject").equals("Modern Art") && Integer.parseInt(row.getAs("year"))>=2007);

		
		sparksession.close();
		
		}
	

}
