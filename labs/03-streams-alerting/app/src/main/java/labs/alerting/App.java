package labs.alerting;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

public final class App {

    private App() {
    }

    public static void main(String[] args) throws InterruptedException {
        AlertConfig defaults = AlertConfig.defaults();
        AlertConfig config = new AlertConfig(
                env("INPUT_TOPIC", defaults.inputTopic()),
                env("ALERT_TOPIC", defaults.alertTopic()),
                Duration.ofSeconds(Long.parseLong(env("WINDOW_SECONDS", String.valueOf(defaults.window().toSeconds())))),
                Duration.ofSeconds(Long.parseLong(env("GRACE_SECONDS", String.valueOf(defaults.grace().toSeconds())))),
                Long.parseLong(env("MIN_PAYMENTS", String.valueOf(defaults.minPayments()))),
                Double.parseDouble(env("RAISE_RATE", String.valueOf(defaults.raiseRate()))),
                Double.parseDouble(env("CLEAR_RATE", String.valueOf(defaults.clearRate()))));

        KafkaStreams streams = new KafkaStreams(AlertingTopology.build(config), properties());
        CountDownLatch done = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close(Duration.ofSeconds(10));
            done.countDown();
        }));
        streams.setUncaughtExceptionHandler(e -> {
            System.err.println("Stream thread failed, replacing it: " + e);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                    .StreamThreadExceptionResponse.REPLACE_THREAD;
        });
        streams.start();
        System.out.println("payment-alerting started: " + config);
        done.await();
    }

    static Properties properties() {
        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, env("APPLICATION_ID", "payment-alerting"));
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, env("BOOTSTRAP_SERVERS", "localhost:9092"));
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, Integer.parseInt(env("REPLICATION_FACTOR", "1")));
        p.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0); // emit every window update; fine for a demo
        p.put(StreamsConfig.STATE_DIR_CONFIG, env("STATE_DIR", "/tmp/kafka-streams"));
        return p;
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
