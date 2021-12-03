package ru.bmstu.hadoop.labs;

import java.io.Serializable;
import org.apache.spark.api.java.function.Function;

public class Flight implements Serializable {
    private float maxDelay;
    private boolean isCancelled;
    private int delayedCount;
    private int cancelledCount;
    private int flightCount;

    public Flight(float maxDelay, boolean isCancelled, int delayedCount, int cancelledCount, int flightCount) {
        this.maxDelay = maxDelay;
        this.isCancelled = isCancelled;
        this.delayedCount = delayedCount;
        this.cancelledCount = cancelledCount;
        this.flightCount = flightCount;
    }

    public static Function createFlight(String delay) {
        boolean isCancelled = delay.isEmpty();
        float maxDelay = isCancelled ? 0 : Float.parseFloat(delay);
        boolean isDelayed = maxDelay > 0;
        //return new Flight(maxDelay, isCancelled, isDelayed ? 1 : 0, isCancelled ? 1 : 0, 1);
        return new Function<String, Flight>() {

            @Override
            public Flight call(String s) throws Exception {
                return null;
            }
        }
    }
}
