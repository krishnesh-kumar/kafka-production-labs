package labs.alerting;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AlertingTopologyTest {

    private static final long T = 1_700_000_000_000L;
    private static final long MINUTE = 60_000L;
    private static final String SHOP = "merchant-17";

    private final AlertConfig config = AlertConfig.defaults(); // 5 min window, >=20 payments, raise 30%, clear 10%
    private TopologyTestDriver driver;
    private TestInputTopic<String, Payment> payments;
    private TestOutputTopic<String, Alert> alerts;

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "alerting-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.STATE_DIR_CONFIG,
                System.getProperty("java.io.tmpdir") + "/alerting-test-" + UUID.randomUUID());
        driver = new TopologyTestDriver(AlertingTopology.build(config), props);
        payments = driver.createInputTopic(config.inputTopic(),
                Serdes.String().serializer(), new JsonSerde<>(Payment.class).serializer());
        alerts = driver.createOutputTopic(config.alertTopic(),
                Serdes.String().deserializer(), new JsonSerde<>(Alert.class).deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    private void pay(String merchant, String status, long timestamp) {
        payments.pipeInput(merchant, new Payment(UUID.randomUUID().toString(), merchant, 1_000, status, timestamp));
    }

    private void payMany(String merchant, String status, int count, long start, long step) {
        for (int i = 0; i < count; i++) {
            pay(merchant, status, start + i * step);
        }
    }

    @Test
    void raisesOnceWhenFailureRateCrossesThreshold() {
        payMany(SHOP, Payment.FAILED, 8, T, 1_000);
        payMany(SHOP, Payment.SUCCEEDED, 12, T + 10_000, 1_000);

        List<Alert> out = alerts.readValuesToList();
        assertEquals(1, out.size(), "exactly one alert");
        Alert alert = out.get(0);
        assertEquals(Alert.RAISED, alert.state());
        assertEquals(SHOP, alert.merchantId());
        assertEquals(20, alert.total());
        assertEquals(8, alert.failed());
        assertEquals(0.4, alert.failureRate());
    }

    @Test
    void doesNotRepeatWhileAlreadyRaised() {
        payMany(SHOP, Payment.FAILED, 8, T, 1_000);
        payMany(SHOP, Payment.SUCCEEDED, 12, T + 10_000, 1_000);
        payMany(SHOP, Payment.FAILED, 15, T + 30_000, 1_000);

        List<Alert> out = alerts.readValuesToList();
        assertEquals(1, out.size(), "still a single RAISED, no duplicates");
    }

    @Test
    void clearsOnceWhenRateRecovers() {
        payMany(SHOP, Payment.FAILED, 8, T, 1_000);
        payMany(SHOP, Payment.SUCCEEDED, 12, T + 10_000, 1_000);
        // Six minutes later the failures have left the 5-minute window; only successes remain.
        payMany(SHOP, Payment.SUCCEEDED, 25, T + 6 * MINUTE, 1_000);

        List<Alert> out = alerts.readValuesToList();
        assertEquals(List.of(Alert.RAISED, Alert.CLEARED), out.stream().map(Alert::state).toList());
    }

    @Test
    void hysteresisPreventsFlapping() {
        payMany(SHOP, Payment.FAILED, 8, T, 1_000);
        payMany(SHOP, Payment.SUCCEEDED, 12, T + 10_000, 1_000);
        // Rate drifts down to ~27% (below raise, above clear): the alert must stay raised, not clear and re-raise.
        payMany(SHOP, Payment.SUCCEEDED, 10, T + 30_000, 1_000);

        List<Alert> out = alerts.readValuesToList();
        assertEquals(List.of(Alert.RAISED), out.stream().map(Alert::state).toList());
    }

    @Test
    void ignoresHighRateOnLowVolume() {
        payMany(SHOP, Payment.FAILED, 10, T, 1_000); // 100% failures, but only 10 payments

        assertTrue(alerts.isEmpty(), "10 payments are not enough to judge a merchant");
    }

    @Test
    void windowEndIsInclusive() {
        // 8 failures at T and 12 successes exactly 5 minutes later share the window [T, T+5m].
        payMany(SHOP, Payment.FAILED, 8, T, 0);
        payMany(SHOP, Payment.SUCCEEDED, 12, T + 5 * MINUTE, 0);

        List<Alert> out = alerts.readValuesToList();
        assertEquals(1, out.size());
        assertEquals(8, out.get(0).failed());
    }

    @Test
    void oneMillisecondPastTheWindowDoesNotCount() {
        payMany(SHOP, Payment.FAILED, 8, T, 0);
        payMany(SHOP, Payment.SUCCEEDED, 12, T + 5 * MINUTE + 1, 0);

        assertTrue(alerts.isEmpty(), "the failures fell out of the window, so volume and rate are both too low");
    }

    @Test
    void merchantsAreIndependent() {
        payMany("merchant-a", Payment.FAILED, 8, T, 1_000);
        payMany("merchant-b", Payment.SUCCEEDED, 20, T, 1_000);
        payMany("merchant-a", Payment.SUCCEEDED, 12, T + 10_000, 1_000);

        List<Alert> out = alerts.readValuesToList();
        assertEquals(1, out.size());
        assertEquals("merchant-a", out.get(0).merchantId());
    }

    @Test
    void lateEventWithinGraceIsCounted() {
        payMany(SHOP, Payment.FAILED, 7, T, 1_000);
        payMany(SHOP, Payment.SUCCEEDED, 12, T + 10_000, 1_000);
        assertTrue(alerts.isEmpty(), "19 payments, below minimum volume");

        // A failure that happened earlier arrives late (inside the 30 s grace period) and completes the picture.
        pay(SHOP, Payment.FAILED, T + 20_000);

        List<Alert> out = alerts.readValuesToList();
        assertEquals(1, out.size(), "late event must update the current window");
        assertEquals(Alert.RAISED, out.get(0).state());
        assertEquals(8, out.get(0).failed());
    }
}
