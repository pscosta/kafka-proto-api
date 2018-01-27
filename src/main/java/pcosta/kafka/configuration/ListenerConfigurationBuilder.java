package pcosta.kafka.configuration;

import pcosta.kafka.api.MessageFilter;
import pcosta.kafka.api.MessageListener;
import pcosta.kafka.api.MessageListenerConfiguration;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;

import java.util.*;

/**
 * This utility class provides a facility for creating new message listener configurations.
 *
 * @author Pedro Costa
 */
public class ListenerConfigurationBuilder<M extends Message> {

    // the upper receiver configuration builder
    private final ReceiverConfigurationBuilder receiverConfigurationBuilder;

    // the builder state
    private Collection<MessageListener> listeners;
    private Collection<String> topics;
    private Class<M> messageType;

    // optional state: filters and extension registry
    private ExtensionRegistry extensionRegistry;
    private Collection<MessageFilter> filters;

    // the default kafka partition
    private int partition = 0;
    // the latest offset
    private long offset = MessageListener.LATEST_OFFSET;

    /**
     * Default protected constructor
     *
     * @param receiverConfigurationBuilder the upper receiver configuration builder
     */
    ListenerConfigurationBuilder(final ReceiverConfigurationBuilder receiverConfigurationBuilder) {
        this.receiverConfigurationBuilder = receiverConfigurationBuilder;
    }

    /**
     * Adds the message listener that shall receive and handle the messages
     *
     * @param newListeners the message listener
     * @return the current builder
     */
    public final ListenerConfigurationBuilder<M> addHandlers(final Collection<MessageListener> newListeners) {
        Objects.requireNonNull(newListeners, "Invalid message listeners provided");
        if (this.listeners == null) {
            this.listeners = new ArrayList<>();
        }
        for (final MessageListener listener : newListeners) {
            Objects.requireNonNull(listener, "Invalid message listeners provided");
            this.listeners.add(listener);
        }
        return this;
    }

    /**
     * Adds the message listeners that shall receive and handle the messages
     *
     * @param listeners the message listeners
     * @return the current builder
     */
    public final ListenerConfigurationBuilder<M> addHandler(final MessageListener... listeners) {
        Objects.requireNonNull(listeners, "Invalid message listeners provided");
        addHandlers(Arrays.asList(listeners));
        return this;
    }

    /**
     * Sets the message deserializer that shall receive the raw byte buffer and translate the message
     *
     * @param messageType the message deserializer
     * @return the current builder
     */
    public final ListenerConfigurationBuilder<M> withMessageType(final Class<M> messageType) {
        Objects.requireNonNull(messageType, "Invalid message type provided");
        if (this.messageType != null && this.messageType != messageType) {
            throw new IllegalStateException("The message type can only be defined once");
        }
        this.messageType = messageType;
        return this;
    }

    /**
     * Supply an {@link ExtensionRegistry} to parse extension fields on the {@code messageType} under configuration
     *
     * @param extensionRegistry the supplied {@link ExtensionRegistry}
     * @return the current builder
     */
    public final ListenerConfigurationBuilder<M> withExtensionRegistry(final ExtensionRegistry extensionRegistry) {
        Objects.requireNonNull(messageType, "Invalid Extension Registry provided");
        this.extensionRegistry = extensionRegistry;
        return this;
    }

    /**
     * Adds the given topics to the current configuration
     *
     * @param topics the message topics
     * @return the current builder
     */
    public final ListenerConfigurationBuilder<M> addTopics(final String... topics) {
        addTopics(Arrays.asList(topics));
        return this;
    }

    /**
     * Adds the given topics to the current configuration
     *
     * @param newTopics the message topics
     * @return the current builder
     */
    public final ListenerConfigurationBuilder<M> addTopics(final Collection<String> newTopics) {
        Objects.requireNonNull(newTopics, "Invalid message topics provided");
        //Assert the topics aren't duplicated
        Set<String> topicsSet = new HashSet<>(newTopics);
        if (topicsSet.size() != newTopics.size()) {
            throw new IllegalArgumentException("Duplicated topics provided");
        }
        if (this.topics == null) {
            this.topics = new HashSet<>();
        }
        for (final String topic : newTopics) {
            Objects.requireNonNull(topic, "Invalid message topic provided");
            this.topics.add(topic);
        }
        return this;
    }

    /**
     * Adds the given filters to the current configuration
     *
     * @param filters the message filters
     * @return the current builder
     */
    public final ListenerConfigurationBuilder<M> addFilters(final Collection<MessageFilter> filters) {
        Objects.requireNonNull(filters, "Invalid message filters provided");
        if (this.filters == null) {
            this.filters = new ArrayList<>();
        }
        for (final MessageFilter filter : filters) {
            Objects.requireNonNull(filter, "Invalid message filter provided");
            this.filters.add(filter);
        }
        return this;
    }

    /**
     * Adds the given filters to the current configuration
     *
     * @param filters the message filters
     * @return the current builder
     */
    public final ListenerConfigurationBuilder<M> addFilter(final MessageFilter... filters) {
        Objects.requireNonNull(filters, "Invalid message filters provided");
        if (this.filters == null) {
            this.filters = new ArrayList<>();
        }
        for (final MessageFilter filter : filters) {
            Objects.requireNonNull(filter, "Invalid message filter provided");
            this.filters.add(filter);
        }
        return this;
    }

    /**
     * Adds the given initial message offset to the current configuration
     *
     * @param offset the initial offset
     * @return the current builder
     */
    public final ListenerConfigurationBuilder<M> addInitialOffset(final long offset) {
        this.offset = offset;
        return this;
    }

    /**
     * Adds the given topic partition to the current configuration
     *
     * @param partition the topic partition
     * @return the current builder
     */
    public final ListenerConfigurationBuilder<M> addTopicPartition(final int partition) {
        this.partition = partition;
        return this;
    }

    /**
     * Builds the listener configuration based on the current builder state.
     *
     * @return the upper receiver configuration
     * @throws NullPointerException if any of the required fields are {@code null}.
     */
    public final ReceiverConfigurationBuilder buildListener() {
        receiverConfigurationBuilder.addListenerConfiguration(internalBuild());
        return receiverConfigurationBuilder;
    }

    /**
     * Internally builds the listener configuration based on the current builder state.
     *
     * @return the listener configuration
     */
    private MessageListenerConfiguration<?> internalBuild() {
        // check listener
        Objects.requireNonNull(listeners, "No message listener(s) has been defined");
        // check topics
        Objects.requireNonNull(topics, "No topics for the listener has been defined");
        // check deserializer
        Objects.requireNonNull(messageType, "No message deserializer has been defined");

        // check nullable fields
        if (filters == null) this.filters = Collections.emptyList();
        if (extensionRegistry == null) this.extensionRegistry = ExtensionRegistry.getEmptyRegistry();

        // create the new configuration
        return new MessageListenerConfigurationImpl<>(listeners, filters, topics, extensionRegistry, messageType, partition, offset);
    }
}
