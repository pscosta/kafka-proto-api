package pcosta.kafka.api;

import com.google.protobuf.Message;

/**
 * @author Pedro Costa
 * <p>
 * defines the contract for all of the message listeners that wish to process incoming messages from the kafka broker
 */
public interface MessageListener<M extends Message> {

    /**
     * The kafka default latest message offset
     */
    long LATEST_OFFSET = -1L;

    /**
     * The kafka default earliest message offset
     */
    long EARLIEST_OFFSET = -3L;

    /**
     * Enable a kafka offset negotiation
     */
    long KAFKA_STORED_OFFSET = -2L;

    /**
     * Handles a received message
     *
     * @param metadata the metadata of the incoming protobuf message to be handled
     * @param message  the message to be handled
     */
    void onMessage(final MessageMetadata metadata, final M message);

    /**
     * Retrieves this Topic's last committed offset
     *
     * @param topic the topic for the offset to be retrieved
     * @return the committed offset
     */
    default long initialOffset(@SuppressWarnings("unused") String topic) {
        return LATEST_OFFSET;
    }
}
