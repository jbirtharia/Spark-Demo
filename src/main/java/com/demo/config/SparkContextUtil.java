package com.demo.config;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkContextUtil {

    public static JavaSparkContext createContext(SparkSession session){
        return new JavaSparkContext(session.sparkContext());
    }
}
