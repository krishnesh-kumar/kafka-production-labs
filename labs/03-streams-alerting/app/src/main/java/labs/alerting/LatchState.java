package labs.alerting;

/** Per-merchant latch kept in a state store, so an alert is raised once and cleared once. */
public record LatchState(boolean active, long lastWindowEnd) {

    static LatchState idle() {
        return new LatchState(false, Long.MIN_VALUE);
    }
}
