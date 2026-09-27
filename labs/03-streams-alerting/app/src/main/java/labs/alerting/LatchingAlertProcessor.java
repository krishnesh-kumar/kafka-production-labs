package labs.alerting;

import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Turns a stream of window updates into RAISED / CLEARED transitions.
 *
 * <p>A sliding-window aggregation emits an update for every window a record touches, including
 * look-ahead windows that end in the future. Only the trailing window ("the last N minutes up to
 * now") answers the question we care about, so we skip windows ending after stream time and
 * windows older than the newest one already evaluated for that merchant.
 *
 * <p>The latch plus separate raise/clear thresholds stop the alert from flapping around a single
 * threshold.
 */
public class LatchingAlertProcessor implements Processor<Windowed<String>, WindowStats, String, Alert> {

    public static final String STORE = "alert-latch";

    private final AlertConfig config;
    private ProcessorContext<String, Alert> context;
    private KeyValueStore<String, LatchState> latches;

    public LatchingAlertProcessor(AlertConfig config) {
        this.config = config;
    }

    @Override
    public void init(ProcessorContext<String, Alert> context) {
        this.context = context;
        this.latches = context.getStateStore(STORE);
    }

    @Override
    public void process(Record<Windowed<String>, WindowStats> record) {
        WindowStats stats = record.value();
        if (stats == null) {
            return;
        }
        String merchant = record.key().key();
        long windowStart = record.key().window().start();
        long windowEnd = record.key().window().end();

        if (windowEnd > context.currentStreamTimeMs()) {
            return; // look-ahead window, not "the last N minutes"
        }

        LatchState latch = latches.get(merchant);
        if (latch == null) {
            latch = LatchState.idle();
        }
        if (windowEnd < latch.lastWindowEnd()) {
            return; // a late event updated an older window; the newer window already decided
        }

        double rate = stats.failureRate();
        boolean active = latch.active();

        if (!active && stats.total >= config.minPayments() && rate >= config.raiseRate()) {
            active = true;
            forward(record, merchant, Alert.RAISED, windowStart, windowEnd, stats, rate);
        } else if (active && rate <= config.clearRate()) {
            active = false;
            forward(record, merchant, Alert.CLEARED, windowStart, windowEnd, stats, rate);
        }

        latches.put(merchant, new LatchState(active, windowEnd));
    }

    private void forward(Record<Windowed<String>, WindowStats> record, String merchant, String state,
                         long windowStart, long windowEnd, WindowStats stats, double rate) {
        Alert alert = new Alert(merchant, state, windowStart, windowEnd, stats.total, stats.failed,
                Math.round(rate * 1000) / 1000.0);
        context.forward(record.withKey(merchant).withValue(alert));
    }
}
