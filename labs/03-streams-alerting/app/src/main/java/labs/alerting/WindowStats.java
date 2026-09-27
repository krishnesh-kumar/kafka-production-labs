package labs.alerting;

/** Running counts for one merchant inside one sliding window. */
public class WindowStats {

    public long total;
    public long failed;

    public WindowStats() {
    }

    public WindowStats(long total, long failed) {
        this.total = total;
        this.failed = failed;
    }

    WindowStats add(Payment payment) {
        total++;
        if (Payment.failed(payment)) {
            failed++;
        }
        return this;
    }

    double failureRate() {
        return total == 0 ? 0.0 : (double) failed / total;
    }
}
