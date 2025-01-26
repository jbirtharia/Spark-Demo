package com.demo.pairRdd;

import java.nio.file.Path;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.demo.config.SparkContextUtil;
import com.demo.config.SparkSessionUtil;

import scala.Tuple2;

public class Spark_07 {
	
	public static void main(String[] args) {

		try (final var sparkSession = SparkSessionUtil.createSession("Spark_07");
				final var sc = SparkContextUtil.createContext(sparkSession)) {
			JavaRDD<String> rdd = sc.textFile(Path.of("src", "main", "resources", "1000words.txt").toString());
			JavaPairRDD<String, Integer> mapToPairRdd = rdd.mapToPair(line -> Tuple2.apply(line, line.length()));
			System.out.println("List of words with length: ");
			mapToPairRdd.collect().forEach(System.out::println);
			JavaPairRDD<String, Integer> counts = mapToPairRdd.reduceByKey(Integer::sum);
			System.out.println("-------------Top 5 elements with words and its length---------------");
			counts.take(5).forEach(tuple -> System.out.println("words: "+tuple._1 + " and length: "+tuple._2));
			
			System.out.println("------------------------Distinct words with length---------------------");
			mapToPairRdd.distinct().take(5).forEach(System.out::println);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
