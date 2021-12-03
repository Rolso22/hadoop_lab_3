package ru.bmstu.hadoop.labs;

import java.io.Serializable;

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

    public static Flight createFlight(String delay) {
        boolean isCancelled = delay.isEmpty();
        float maxDelay = isCancelled? 0 : Float.parseFloat(delay);
        boolean isDelayed = 
    }
}
