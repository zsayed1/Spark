package com.virtualpairprogrammers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class SparkSQLColumnFiltering {
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
//		Dataset<Row> moderArtResults = dataset.filter(row-> row.getAs("subject").equals("Modern Art") && Integer.parseInt(row.getAs("year"))>=2007);
		// Saving a column in to a column
		
		Column subjectColumn = dataset.col("subject");
		Column yearColumn = dataset.col("year");
		
		Dataset<Row> moderArt2007 = dataset.filter(subjectColumn.equalTo("Modern Art").and(yearColumn.geq(2007)));
		moderArt2007.show();
		sparksession.close();
		
		// we can also use fucntions instead of datasets
//		Column subjectColumn1 = functions.col("subject");
//		Column yearColumn1 = functions.col("year");

//		 Once made static we can write it as
		Column subjectColumn1 = col("subject");
		Column yearColumn1 = col("year");
		
		
		// And When the fucntions are made static by addid a static 
//		 Then replace the column name with the col() fucntion directly
		Dataset<Row> moderArt2007Static = dataset.filter(col("subject").equalTo("Modern Art").and(col("year").geq(2007)));
		moderArt2007Static.show();
		sparksession.close();
}
}