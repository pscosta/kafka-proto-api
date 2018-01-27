package pcosta.kafka.api;

/**
 * @author Pedro Costa
 * <p>
 * Metadata for an incoming Protobuf Message
 */
public interface MessageMetadata {

    /**
     * Returns the received message's kafka offset
     *
     * @return the message's offset
     */
    long getOffset();

    /**
     * Returns the received kafka message key
     *
     * @return the message key
     */
    MessageKey getKey();

    /**
     * Returns the incoming message source topic
     *
     * @return the incoming message source topic
     */
    String getSrcTopic();

    /**
     * Returns incoming message traceability identifier
     *
     * @return the incoming message traceability id
     */
    String getTraceabilityId();
}
