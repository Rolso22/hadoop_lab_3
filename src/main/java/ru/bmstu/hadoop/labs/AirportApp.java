package ru.bmstu.hadoop.labs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportApp {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("AirportApp_lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> airportFile = sc.textFile("L_AIRPORT_ID.csv");
        JavaPairRDD<String, String> airportMap = airportFile.flatMapToPair(str -> {
                    String[] lineParts = str.split(",");
                    
                }
        )
    }
}
