package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class FullSparkSQL {
	public static void main(String args[]) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder()
				.appName("FullSparkSQL")
				.master("local[*]")
				.config("spark.warehouse.dir","file:///tmp/")
				.getOrCreate();
		
		Dataset<Row> readDataset = spark
				.read()
				.option("header",true)
				.csv("src/main/resources/exams/students.csv");
		readDataset.createOrReplaceTempView("student_table");
		Dataset<Row> sqlDataset = spark.sql("Select score from student_table where 'subject'='Modern Art'");
		
		sqlDataset.show();
		
		spark.close();
		
		
	}
}
