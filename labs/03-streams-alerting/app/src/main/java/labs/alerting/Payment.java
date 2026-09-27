package labs.alerting;

/** A card or wallet payment attempt. Money is in minor units; timestamp is event time in epoch millis. */
public record Payment(String paymentId, String merchantId, long amountMinor, String status, long timestamp) {

    public static final String FAILED = "FAILED";
    public static final String SUCCEEDED = "SUCCEEDED";

    static boolean failed(Payment p) {
        return FAILED.equals(p.status());
    }
}
