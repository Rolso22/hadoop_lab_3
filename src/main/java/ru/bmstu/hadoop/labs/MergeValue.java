package ru.bmstu.hadoop.labs;

import org.apache.spark.api.java.function.Function;

public class MergeValue implements Function<Flight, String, Flight> {

    @Override
    public Flight call(Flight f, String s) throws Exception {
        return null;
    }
}