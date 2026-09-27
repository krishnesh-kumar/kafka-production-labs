package labs.alerting;

import java.time.Duration;

/**
 * Thresholds for the alerting topology.
 *
 * @param window      length of the sliding window ("the last 5 minutes")
 * @param grace       how late an event may arrive and still be counted
 * @param minPayments minimum volume in the window before the rate means anything
 * @param raiseRate   failure rate that raises the alert
 * @param clearRate   failure rate that clears it; lower than raiseRate so the alert does not flap
 */
public record AlertConfig(String inputTopic, String alertTopic, Duration window, Duration grace,
                          long minPayments, double raiseRate, double clearRate) {

    public static AlertConfig defaults() {
        return new AlertConfig("payments", "merchant-alerts",
                Duration.ofMinutes(5), Duration.ofSeconds(30), 20, 0.30, 0.10);
    }

    public AlertConfig {
        if (clearRate >= raiseRate) {
            throw new IllegalArgumentException("clearRate must be below raiseRate (hysteresis)");
        }
    }
}
