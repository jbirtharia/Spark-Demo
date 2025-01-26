package com.demo.tupples;

import java.nio.file.Path;

import org.apache.spark.api.java.JavaRDD;

import com.demo.config.SparkContextUtil;
import com.demo.config.SparkSessionUtil;

import scala.Tuple2;

public class Spark_06 {
	
	public static void main(String[] args) {

		try (final var sparkSession = SparkSessionUtil.createSession("Spark_06");
				final var sc = SparkContextUtil.createContext(sparkSession)) {
			JavaRDD<String> rdd = sc.textFile(Path.of("src", "main", "resources", "1000words.txt").toString());
			JavaRDD<Tuple2<String, Integer>> tuppleMap = rdd.map(line -> Tuple2.apply(line, line.length()));
			System.out.println("List of words with length: ");
			tuppleMap.collect().forEach(System.out::println);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
