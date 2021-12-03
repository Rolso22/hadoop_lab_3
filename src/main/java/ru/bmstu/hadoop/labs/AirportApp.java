package ru.bmstu.hadoop.labs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Map;

public class AirportApp {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("AirportApp_lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> airportFile = sc.textFile("L_AIRPORT_ID.csv");
        Map<String, String> airportMap = airportFile
                .filter(str -> str)
                .mapToPair(str -> {
                    String[] lineParts = str.split(",");
                    return new Tuple2<>(lineParts[0], lineParts[1]);
                }).collectAsMap();

        JavaRDD<String> flightsFile = sc.textFile("664600583_T_ONTIME_sample.csv");
        JavaPairRDD<Tuple2<String, String>, Flight> flights = flightsFile.flatMapToPair(str -> {
            String[] lineParts = str.split(",");

        });
    }
}
