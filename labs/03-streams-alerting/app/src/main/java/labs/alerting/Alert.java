package labs.alerting;

/** Emitted once when a merchant's failure rate crosses the raise threshold, and once when it recovers. */
public record Alert(String merchantId, String state, long windowStart, long windowEnd,
                    long total, long failed, double failureRate) {

    public static final String RAISED = "RAISED";
    public static final String CLEARED = "CLEARED";
}
