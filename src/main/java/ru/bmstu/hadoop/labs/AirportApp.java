package ru.bmstu.hadoop.labs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import static ru.bmstu.hadoop.labs.Constants.*;
import java.util.Map;

public class AirportApp {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("AirportApp_lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> airportFile = sc.textFile("L_AIRPORT_ID.csv");
        Map<String, String> airportMap = airportFile
                .filter(str -> !str.contains(CODE))
                .mapToPair(str -> {
                    String[] lineParts = str.split(DELIMITER_COMMA);
                    return new Tuple2<>(lineParts[CODE_INDEX], lineParts[DESCRIPTION_INDEX]);
                }).collectAsMap();

        JavaRDD<String> flightsFile = sc.textFile("664600583_T_ONTIME_sample.csv");
        JavaPairRDD<Tuple2<String, String>, Flight> flights = flightsFile
                .filter(str -> !str.contains(YEAR))
                .mapToPair(str -> {
                    String[] lineParts = str.split(DELIMITER_COMMA);
                    String originPort = lineParts[ORIGIIN_AIRPORT];
                    String destPort = lineParts[DEST_AIRPORT];
                    String delay = lineParts[DELAY_TIME_INDEX];
                    return new Tuple2<>(new Tuple2<>(originPort, destPort), delay);
                })
                .combineByKey(new CreateCombiner(), new MergeValue(), new MergeCombiners());
        final Broadcast<Map<String, String>> airportsBroadcasted = sc.broadcast(airportMap);
        JavaRDD<String> result = flights.map()
    }

}
