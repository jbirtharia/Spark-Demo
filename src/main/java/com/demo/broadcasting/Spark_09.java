package com.demo.broadcasting;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;

import com.demo.config.SparkContextUtil;
import com.demo.config.SparkSessionUtil;

public class Spark_09 {
	
	public static void main(String[] args) {

		try (final var sparkSession = SparkSessionUtil.createSession("Spark_09");
				final var sc = SparkContextUtil.createContext(sparkSession)) {

			// Creating broadcast variables
			Broadcast<Map<String, String>> broadcastStocksListing = sc.broadcast(getStocksListing());
			Broadcast<Map<String, Double>> broadcastStocksPricing = sc.broadcast(getStocksPricing());
			try {

				List<String> stocks = List.of("APPL", "META", "TESL", "GOGGL");
				JavaRDD<String> stocksRdd = sc.parallelize(stocks);
				JavaRDD<String> result = stocksRdd.map(s -> {
					// Getting values from broadcast variables
					String stockName = broadcastStocksListing.getValue().get(s);
					Double price = broadcastStocksPricing.getValue().get(s);
					return String.format("Stock name " + stockName + " having price: " + price);
				});
				result.collect().forEach(System.out::println);
			} finally {
				// Destroying broadcast variable so broadcast variable release resources
				if (null != broadcastStocksListing) {
					broadcastStocksListing.destroy();
				}
				if (null != broadcastStocksPricing) {
					broadcastStocksPricing.destroy();
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static Map<String, Double> getStocksPricing() {
		Map<String, Double> stocksPricing = new HashMap<>();
		stocksPricing.put("APPL", 112.20);
		stocksPricing.put("META", 110.01);
		stocksPricing.put("TESL", 150.30);
		stocksPricing.put("GOGGL", 115.90);
		return stocksPricing;
	}

	private static Map<String, String> getStocksListing() {
		Map<String, String> stocksListing = new HashMap<>();
		stocksListing.put("APPL", "Apple INC");
		stocksListing.put("META", "META Platform INC");
		stocksListing.put("TESL", "TESLA INC");
		stocksListing.put("GOGGL", "Albhabet INC");
		return stocksListing;
	}

}
