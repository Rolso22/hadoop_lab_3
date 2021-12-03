package ru.bmstu.hadoop.labs;

import org.apache.spark.api.java.function.Function2;

public class MergeValue implements Function2<Flight, String, Flight> {

    @Override
    public Flight call(Flight flight, String delay) throws Exception {
        return flight.mergeValues(delay);
    }
}