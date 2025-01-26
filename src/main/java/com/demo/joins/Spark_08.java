package com.demo.joins;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;

import com.demo.config.SparkContextUtil;
import com.demo.config.SparkSessionUtil;

import scala.Tuple2;

public class Spark_08 {

	public static void main(String[] args) {

		try (final var sparkSession = SparkSessionUtil.createSession("Spark_06");
				final var sc = SparkContextUtil.createContext(sparkSession)) {
			JavaPairRDD<Integer, String> customersRdd = sc.parallelizePairs(getCustomers());
			JavaPairRDD<Integer, Double> billsRdd = sc.parallelizePairs(getBills());
			// Only join those records whose are avilable in both rdd's
			JavaPairRDD<Integer, Tuple2<String, Double>> innerJoinRdd = customersRdd.join(billsRdd);
			innerJoinRdd.collect().forEach(System.out::println);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static List<Tuple2<Integer, Double>> getBills() {

		return List.of(

				Tuple2.apply(1, 100.01), Tuple2.apply(2, 90.09), Tuple2.apply(3, 30.02), Tuple2.apply(4, 150.04),
				Tuple2.apply(5, 120.89), Tuple2.apply(6, 102.80), Tuple2.apply(7, 50.80)

		);
	}

	private static List<Tuple2<Integer, String>> getCustomers() {

		return List.of(

				Tuple2.apply(1, "Sachin"), Tuple2.apply(2, "Mahi"), Tuple2.apply(3, "Rahul"),
				Tuple2.apply(4, "Ajinkya"), Tuple2.apply(5, "Yuvraj"), Tuple2.apply(6, "Shami"),
				Tuple2.apply(7, "Zaheer"), Tuple2.apply(8, "Laxman")

		);
	}
}
