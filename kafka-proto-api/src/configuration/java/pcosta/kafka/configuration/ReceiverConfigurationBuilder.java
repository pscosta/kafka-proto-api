package pcosta.kafka.configuration;

import pcosta.kafka.api.*;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;

/**
 * This utility class provides a facility for creating new message receiver configurations.
 *
 * @author Pedro Costa
 */
public class ReceiverConfigurationBuilder {

    private static final Logger log = LoggerFactory.getLogger(ReceiverConfigurationBuilder.class);

    // the listeners configuration
    private final Collection<MessageListenerConfiguration<?>> listeners = new HashSet<>();

    // the error listener
    private PlatformErrorListener errorListener;

    /**
     * Creates a new receiver configuration builder instance in order to create a brand new receiver configuration.
     *
     * @return the receiver configuration builder
     */
    public static ReceiverConfigurationBuilder newBuilder() {
        return new ReceiverConfigurationBuilder();
    }

    /**
     * Defines the actual listener that shall receive and handle the platform messages and handle the errors.
     *
     * @param listener the message listener
     * @return the current builder
     */
    public final ReceiverConfigurationBuilder withErrorListener(final PlatformErrorListener listener) {
        Objects.requireNonNull(listener, "Invalid error listener provided");
        if (this.errorListener != null && this.errorListener != listener) {
            throw new IllegalStateException("the error listener can only be defined once");
        }
        this.errorListener = listener;
        return this;
    }

    /**
     * Creates a new, empty message listener configuration
     *
     * @param <M> the type of the messages to be handled by the listener
     * @return the message listener configuration
     */
    public <M extends Message> ListenerConfigurationBuilder<M> newListener() {
        return new ListenerConfigurationBuilder<>(this);
    }

    /**
     * Creates and immediately builds a new listener with the specified configuration.
     * <p>
     * This is a short for a simple listener creation.
     *
     * @param listener    the message listener to be registered
     * @param messageType the supported message type
     * @param topics      the topics to be tracked
     * @param <M>         the type of the messages to be handled by the listener
     * @return the current builder
     */
    public <M extends Message> ReceiverConfigurationBuilder newListener(final MessageListener<M> listener,
                                                                        final Class<M> messageType,
                                                                        final String... topics) {
        new ListenerConfigurationBuilder<M>(this)
                .addTopics(topics)
                .addHandler(listener)
                .withMessageType(messageType)
                .buildListener();

        return this;
    }

    /**
     * Creates and immediately builds a new listener with the specified configuration.
     * <p>
     * This is a short for a simple listener creation.
     *
     * @param listener      the message listener to be registered
     * @param messageType   the supported message type
     * @param initialOffset the messages initial offset
     * @param partition     the topic partition
     * @param topics        the topics to be tracked
     * @param <M>           the type of the messages to be handled by the listener
     * @return the current builder
     */
    public <M extends Message> ReceiverConfigurationBuilder newListener(final MessageListener<M> listener,
                                                                        final Class<M> messageType,
                                                                        final long initialOffset,
                                                                        final int partition,
                                                                        final String... topics) {
        new ListenerConfigurationBuilder<M>(this)
                .addTopics(topics)
                .addHandler(listener)
                .addInitialOffset(initialOffset)
                .addTopicPartition(partition)
                .withMessageType(messageType)
                .buildListener();

        return this;
    }

    /**
     * Builds the receiver configuration based on the current builder state.
     *
     * @return the receiver configuration
     */
    public MessageReceiverConfiguration build() {
        // check if we do have any listeners defined
        if (listeners.isEmpty()) {
            log.warn("no message listeners have been defined");
        }

        // validate error listener
        Objects.requireNonNull(errorListener, "No error listener have been defined");

        return new MessageReceiverConfigurationImpl(errorListener, listeners);
    }

    /**
     * Adds the specified listener configuration to the current receiver configuration
     *
     * @param listenerConfiguration the listener configuration
     */
    void addListenerConfiguration(final MessageListenerConfiguration<?> listenerConfiguration) {
        listeners.add(listenerConfiguration);
    }
}
