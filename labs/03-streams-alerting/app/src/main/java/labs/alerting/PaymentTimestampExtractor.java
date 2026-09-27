package labs.alerting;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/** Use the payment's own event time, not the time Kafka received it. Windows then match reality. */
public class PaymentTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() instanceof Payment payment && payment.timestamp() > 0) {
            return payment.timestamp();
        }
        return record.timestamp() >= 0 ? record.timestamp() : partitionTime;
    }
}
