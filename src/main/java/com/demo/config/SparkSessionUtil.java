package com.demo.config;


import org.apache.spark.sql.SparkSession;

public class SparkSessionUtil {

    public static SparkSession createSession(String appName) {

        return SparkSession.builder().appName(appName).master("local[*]").getOrCreate();
    }

}
