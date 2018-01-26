package pcosta.kafka.internal;

import pcosta.kafka.api.MessageKey;
import pcosta.kafka.api.MessageMetadata;

/**
 * @author Pedro Costa
 * <p>
 * Metadata implementation for an incoming kafka Message
 */
public class KafkaMetadata implements MessageMetadata {

    private final String srcTopic;
    private final MessageKey key;
    private final long offset;
    private final String traceabilityId;

    /**
     * The default constructor
     *
     * @param srcTopic the incoming message origin topic
     * @param key      the incoming message kafka key
     * @param offset   the incoming message kafka offset
     */
    public KafkaMetadata(String srcTopic, MessageKey key, long offset, String traceabilityId) {
        this.srcTopic = srcTopic;
        this.key = key;
        this.offset = offset;
        this.traceabilityId = traceabilityId;
    }

    @Override
    public MessageKey getKey() {
        return key;
    }

    @Override
    public String getSrcTopic() {
        return srcTopic;
    }

    @Override
    public long getOffset() {
        return offset;
    }

    @Override
    public String getTraceabilityId() {
        return traceabilityId;
    }
}
