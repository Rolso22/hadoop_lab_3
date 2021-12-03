package ru.bmstu.hadoop.labs;

import org.apache.spark.api.java.function.Function2;

public class MergeCombiners implements Function2<Flight, Flight, Flight> {

    @Override
    public Flight call(Flight flight1, Flight flight2) throws Exception {
        return flight1.mergeCombine(flight2);
    }
}
