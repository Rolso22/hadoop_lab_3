package ru.bmstu.hadoop.labs;


import org.apache.spark.api.java.function.Function;

public class CreateCombiner implements Function<String, Flight> {

    @Override
    public Flight call(String delay) throws Exception {
        return Flight.createFlight(delay);
    }
}
