package pcosta.kafka.api;

/**
 * The MessageProducer sends a message to a single or multiple Kafka {@code Topic}s
 *
 * @author Pedro Costa
 */
public interface MessageProducer<M> {

    /**
     * Sends the given {@code message} to the given {@code topics} with a default message Key:
     * {@code <topic name>|<fully qualified message name>}
     *
     * @param message The message to be sent
     * @param topics  The destination topics where the message is to be placed
     * @throws MessagingException if the message can't be sent to the underlying messaging service and topics.
     */
    void send(M message, String... topics) throws MessagingException;

    /**
     * @param message The message to be sent
     * @param key     The message key
     * @param topics  The destination topics where the message is to be placed
     * @throws MessagingException if the message can't be sent to the underlying messaging service and topics.
     */
    void send(M message, String key, String... topics) throws MessagingException;
}
