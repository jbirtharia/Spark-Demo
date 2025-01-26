package com.demo.read_text_file;

import org.apache.spark.api.java.JavaRDD;

import com.demo.config.SparkContextUtil;
import com.demo.config.SparkSessionUtil;

public class Spark_02 {
	
	public static void main(String[] args) {
		
		 try (final var sparkSession = SparkSessionUtil.createSession("Spark_02");
	             final var sc = SparkContextUtil.createContext(sparkSession)) {
			 
			 	JavaRDD<String> textFile = sc.textFile("src/main/resources/1000words.txt");
			 	System.out.println("Count: "+textFile.count());
			 	System.out.println("First 10 lines of file: ");
			 	textFile.take(10).forEach(System.out::println);
	            
	            Thread.sleep(100000000);

	        }catch (Exception e){
	            e.printStackTrace();
	        }
		
	}

}
