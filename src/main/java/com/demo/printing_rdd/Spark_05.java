package com.demo.printing_rdd;

import java.nio.file.Path;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import com.demo.config.SparkContextUtil;
import com.demo.config.SparkSessionUtil;

public class Spark_05 {

	public static void main(String[] args) {

		try (final var sparkSession = SparkSessionUtil.createSession("Spark_05");
				final var sc = SparkContextUtil.createContext(sparkSession)) {
			JavaRDD<String> rdd = sc.textFile(Path.of("src", "main", "resources", "magna-carta.txt.gz").toString());
			JavaRDD<String> words = rdd.flatMap(line -> List.of(line.split("//s")).iterator());
			System.out.println("Words count: " + words.count());

			// Below line will throw exception, because below foreach is from
			// org.apache.spark package.
			// We can only call forach in executors nodes, not in cluster nodes. So in below
			// lines will get the exeption
			// words.foreach(System.out::println);

			// Below line is printing after calling collect and then forEach. This forEach
			// we can call.
			// This forEach is of java.lang package.
			// words.collect().forEach(System.out::println);

			// To print first 10 lines
			System.out.println("First 10 lines of file: ");
			words.take(10).forEach(System.out::println);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
