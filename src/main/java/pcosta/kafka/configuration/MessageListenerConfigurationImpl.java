package pcosta.kafka.configuration;

import pcosta.kafka.api.MessageFilter;
import pcosta.kafka.api.MessageListener;
import pcosta.kafka.api.MessageListenerConfiguration;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;

import java.util.Collection;

/**
 * A simple implementation of the {@link MessageListenerConfiguration}, which receives it's configuration upon construction time.
 *
 * @author Pedro Costa
 */
class MessageListenerConfigurationImpl<M extends Message> implements MessageListenerConfiguration<M> {

    private final Collection<MessageListener> listeners;
    private final Collection<String> topics;
    private final Class<M> messageType;
    private final Collection<MessageFilter> filters;
    private final ExtensionRegistry extensionRegistry;
    private final int partition;
    private final long offset;

    /**
     * Default configuration constructor.
     *
     * @param listeners         the actual message listener
     * @param topics            the collection of message topics
     * @param messageType       the messages type
     * @param extensionRegistry the extension registry used to parse extension fields
     * @param partition         the topic partition
     * @param offset            the initial message offset
     */
    MessageListenerConfigurationImpl(final Collection<MessageListener> listeners,
                                     final Collection<MessageFilter> filters,
                                     final Collection<String> topics,
                                     final ExtensionRegistry extensionRegistry,
                                     final Class<M> messageType, int partition, long offset) {
        this.listeners = listeners;
        this.topics = topics;
        this.messageType = messageType;
        this.filters = filters;
        this.extensionRegistry = extensionRegistry;
        this.partition = partition;
        this.offset = offset;
    }

    @Override
    public Collection<MessageListener> getMessageListeners() {
        return listeners;
    }

    @Override
    public Collection<String> getTopics() {
        return topics;
    }

    @Override
    public Class<M> getMessageType() {
        return messageType;
    }

    @Override
    public Collection<MessageFilter> getMessageFilters() {
        return filters;
    }

    @Override
    public int getPartition() {
        return partition;
    }

    @Override
    public long getOffset() {
        return offset;
    }

    @Override
    public ExtensionRegistry getExtensionRegistry() {
        return extensionRegistry;
    }
}
