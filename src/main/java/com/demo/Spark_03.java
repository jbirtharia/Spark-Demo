package com.demo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.demo.config.SparkSessionUtil;

public class Spark_03 {

	
	public static void main(String[] args) {
		
		 try (final var sparkSession = SparkSessionUtil.createSession("Spark_03")) {
			 
		        Dataset<Row> df = sparkSession.read()
		        		.parquet("src/main/resources/flights-1m.parquet");

		        // Display the DataFrame
		        df.show();

	        }catch (Exception e){
	            e.printStackTrace();
	        }
		
	}
}
