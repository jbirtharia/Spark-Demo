package com.demo.perfomance;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.api.java.JavaRDD;

import com.demo.config.SparkContextUtil;
import com.demo.config.SparkSessionUtil;

public class Spark_04 {
	
	
	public static void main(String[] args) {

		try (final var sparkSession = SparkSessionUtil.createSession("Spark_04");
				final var sc = SparkContextUtil.createContext(sparkSession)) {

			// 12 is number of partition
			JavaRDD<String> rdd = sc.parallelize(getData(),12);
			Instant start = Instant.now();
			for (int i = 0; i < 10; i++) {
				// Time taken: 159
				long strLength = rdd.map(r -> r.length()).count();
				System.out.println("Total length: " + strLength);
			}
			long timeElappsed = (Duration.between(start, Instant.now())).toMillis() / 10;
			System.out.println("Time taken: " + timeElappsed);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static List<String> getData() {

		final var dataSize = 100000;

		List<String> data = new ArrayList<>();

		for (int i = 0; i < dataSize; i++) {
			data.add(RandomStringUtils.randomAscii(ThreadLocalRandom.current().nextInt(10)));
		}
		return data;
	}
}
