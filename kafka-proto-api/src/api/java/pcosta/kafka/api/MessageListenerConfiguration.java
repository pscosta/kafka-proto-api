package pcosta.kafka.api;

import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;

import java.util.Collection;

/**
 * The configuration for the message listeners.
 * <p>
 * This configuration object is used along side with the {@link MessageReceiverConfiguration} overall configuration.
 *
 * @param <M> the type of the messages to be received
 * @author Pedro Costa
 */
public interface MessageListenerConfiguration<M extends Message> {

    /**
     * Returns the topics to listen for the incoming messages
     *
     * @return the component topic
     */
    Collection<String> getTopics();

    /**
     * Returns the actual message listener that will receive and process the messages
     *
     * @return the {@link Collection} containing the message listeners
     */
    Collection<MessageListener> getMessageListeners();

    /**
     * Returns the message type of the messages to be received
     *
     * @return the message type
     */
    Class<M> getMessageType();

    /**
     * Returns the message filters to be applied before message delivery.
     * Only messages that are compliant with all the defined filters are indeed delivered.
     *
     * @return the collection of filters
     */
    Collection<MessageFilter> getMessageFilters();

    /**
     * The default message partition for the configured {@code Topic}s
     *
     * @return the configured partition. Partition {@code 0} will be returned by default
     */
    int getPartition();

    /**
     * Used retrieve all messages before a certain offset.
     * Specify {@code -1} to receive the latest offset (i.e. the offset of the next coming message)
     * and {@code -2} to receive the earliest available offset.
     *
     * @return the configured offset. The {@link MessageListener#LATEST_OFFSET} offset will be used by default
     */
    long getOffset();

    /**
     * The extension registry used to parse extension fields
     *
     * @return the provided extension registry
     */
    ExtensionRegistry getExtensionRegistry();
}
