package ru.bmstu.hadoop.labs;

import scala.Tuple2;
import java.io.Serializable;
import java.util.Map;

public class Flight implements Serializable {
    private float maxDelay;
    private int delayedCount;
    private int cancelledCount;
    private int flightCount;

    public Flight(float maxDelay, int delayedCount, int cancelledCount, int flightCount) {
        this.maxDelay = maxDelay;
        this.delayedCount = delayedCount;
        this.cancelledCount = cancelledCount;
        this.flightCount = flightCount;
    }

    public static Flight createFlight(String delay) {
        boolean isCancelled = delay.isEmpty();
        float maxDelay = isCancelled ? 0 : Float.parseFloat(delay);
        boolean isDelayed = maxDelay > 0;
        return new Flight(maxDelay, isDelayed ? 1 : 0, isCancelled ? 1 : 0, 1);
    }

    public Flight mergeCombine(Flight flight) {
        maxDelay = Math.max(flight.maxDelay, maxDelay);
        flightCount++;
        delayedCount += flight.delayedCount;
        cancelledCount += flight.cancelledCount;
        return this;
    }

    public Flight mergeValues(String delay) {
        Flight flight = Flight.createFlight(delay);
        return this.mergeCombine(flight);
    }

    public static String getResult(Tuple2<Tuple2<String, String>, Flight> ports, Map<String, String> map) {
        return null;
    }
}
