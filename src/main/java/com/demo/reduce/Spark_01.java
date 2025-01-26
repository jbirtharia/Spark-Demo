package com.demo.reduce;

import java.util.stream.Stream;

import com.demo.config.SparkContextUtil;
import com.demo.config.SparkSessionUtil;

public class Spark_01 {

    public static void main(String[] args) {

        try (final var sparkSession = SparkSessionUtil.createSession("Spark_01");
             final var sc = SparkContextUtil.createContext(sparkSession)) {

            final var data = Stream.iterate(1, n -> n + 1).limit(10).toList();
            final var rdd = sc.parallelize(data);
            System.out.println("Count: " + rdd.count());
            System.out.println("Number of partitions: "+rdd.getNumPartitions());
            
            Integer max = rdd.reduce(Integer::max);
            System.out.println("Max Element: "+max);
            
            Thread.sleep(100000000);

        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
