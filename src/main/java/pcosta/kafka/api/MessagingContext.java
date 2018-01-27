package pcosta.kafka.api;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Collection;

/**
 * The Kafka messaging context, in which message producers and receivers can be created in order to interact with kafka brokers
 *
 * @author Pedro Costa
 */
public interface MessagingContext {

    /**
     * Creates and configures the messaging receiver with the specified listener configuration.
     * <p>
     * This method <b>should only be called once</b> while initializing/configuring during the bootstrap.
     *
     * @param configuration the receiver final configuration
     * @throws MessagingException if any error occurs while either creating or registering the message receiver.
     */
    void createReceiver(final MessageReceiverConfiguration configuration) throws MessagingException;

    /**
     * Creates a MessageProducer bounded to a specific type of messages, denoted by the {@code valueSerializer}.
     *
     * @param producerKey     the unique message producer key
     * @param keySerializer   the messages key deserializer
     * @param valueSerializer the messages value deserializer
     * @param <M>             the message type
     * @return the message producer
     * @throws MessagingException if any error occurs while either creating or registering the message producer.
     */
    <M> MessageProducer<M> createProducer(final String producerKey,
                                          final Serializer keySerializer,
                                          final Serializer valueSerializer) throws MessagingException;

    /**
     * Creates a MessageProducer bounded to a specific type of messages, denoted by the {@code valueSerializer}.
     *
     * @param producerKey     the unique message producer key
     * @param keySerializer   the messages key deserializer
     * @param valueSerializer the messages value deserializer
     * @param filters         the message filters to be applied to incoming messages
     * @param <M>             the message type
     * @return the message producer
     * @throws MessagingException if any error occurs while either creating or registering the message producer.
     */
    <M> MessageProducer<M> createProducer(final String producerKey,
                                          final Serializer keySerializer,
                                          final Serializer valueSerializer,
                                          final Collection<MessageFilter> filters) throws MessagingException;

    /**
     * Closes this context resources.
     * All receivers must be removed and the application module must be terminated.
     *
     * @throws MessagingException if an unexpected error occurs while freeing the resources
     */
    void shutdown() throws MessagingException;
}
