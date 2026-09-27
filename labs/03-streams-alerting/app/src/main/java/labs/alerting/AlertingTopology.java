package labs.alerting;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

/**
 * payments --(key by merchant)--> sliding-window counts --> latch --> merchant-alerts
 */
public final class AlertingTopology {

    private AlertingTopology() {
    }

    public static Topology build(AlertConfig config) {
        Serde<String> keys = Serdes.String();
        Serde<Payment> payments = new JsonSerde<>(Payment.class);
        Serde<WindowStats> stats = new JsonSerde<>(WindowStats.class);
        Serde<Alert> alerts = new JsonSerde<>(Alert.class);
        Serde<LatchState> latch = new JsonSerde<>(LatchState.class);

        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(LatchingAlertProcessor.STORE), keys, latch));

        builder.stream(config.inputTopic(),
                        Consumed.with(keys, payments).withTimestampExtractor(new PaymentTimestampExtractor()))
                .filter((key, payment) -> payment != null && payment.merchantId() != null)
                .selectKey((key, payment) -> payment.merchantId())
                .groupByKey(Grouped.<String, Payment>as("by-merchant").withKeySerde(keys).withValueSerde(payments))
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(config.window(), config.grace()))
                .aggregate(WindowStats::new,
                        (merchant, payment, acc) -> acc.add(payment),
                        Materialized.<String, WindowStats, WindowStore<Bytes, byte[]>>as("merchant-window-stats")
                                .withKeySerde(keys)
                                .withValueSerde(stats))
                .toStream()
                .process(() -> new LatchingAlertProcessor(config), LatchingAlertProcessor.STORE)
                .to(config.alertTopic(), Produced.with(keys, alerts));

        return builder.build();
    }
}
