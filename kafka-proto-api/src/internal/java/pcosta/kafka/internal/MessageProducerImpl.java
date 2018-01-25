package pcosta.kafka.internal;

import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pcosta.kafka.api.MessageFilter;
import pcosta.kafka.api.MessageProducer;
import pcosta.kafka.api.MessagingException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Pedro Costa
 * <p/>
 * Responsible for serializing and sending protobuf messages to kafka broker
 */
@SuppressWarnings("unchecked")
final class MessageProducerImpl<M> implements MessageProducer<M> {

    private static final Logger log = LoggerFactory.getLogger(MessageProducerImpl.class);

    // the message deserializer
    private final Serializer valueSerializer;
    // the key deserializer
    private final Serializer keySerializer;
    // the source topic
    private final String srcTopic;
    // the message filters
    private final Collection<MessageFilter> filters;
    // the kafka senders map by topic
    private Map<String, KafkaSender<String, M>> kafkaSenders;

    /**
     * Default producer constructor
     *
     * @param keySerializer   the key deserializer
     * @param valueSerializer the message deserializer
     */
    MessageProducerImpl(String srcTopic, Serializer keySerializer, Serializer valueSerializer) {
        this(srcTopic, keySerializer, valueSerializer, Collections.emptyList());
    }

    /**
     * The constructor with filters to be applied to outgoing messages
     *
     * @param keySerializer   the key deserializer
     * @param valueSerializer the message deserializer
     */
    MessageProducerImpl(String srcTopic, Serializer keySerializer, Serializer valueSerializer, Collection<MessageFilter> filters) {
        this.srcTopic = srcTopic;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.kafkaSenders = new ConcurrentHashMap<>();
        this.filters = new ArrayList<>(filters);
    }

    @Override
    public void send(M message, String... topics) throws MessagingException {
        send(message, null, topics);
    }

    @Override
    public void send(M message, String key, final String... topics) throws MessagingException {
        Objects.requireNonNull(topics, "Registered Invalid topics");

        // transform message
        log.debug("Transforming object {}", message);

        for (String topic : topics) {
            Objects.requireNonNull(topic, "Invalid topic");
            // kafka sender lazy-instantiation for this topic
            if (null == kafkaSenders.get(topic)) {
                synchronized (MessageProducerImpl.class) {
                    if (null == kafkaSenders.get(topic))
                        kafkaSenders.put(topic, new KafkaSender<>(srcTopic, topic, keySerializer, valueSerializer));
                }
            }
        }

        // log the message - wrap around if clause due to message.toString() (can be expensive)
        if (log.isDebugEnabled()) {
            log.debug("sending message to kafka broker:{}" + "topics: {}{}" + "payload: {}{}",
                    System.lineSeparator(),
                    topics, System.lineSeparator(),
                    message.toString(), System.lineSeparator()
            );
        }

        // send the message for each destination
        for (final String dstTopic : topics) {
            // generate the key for this message according with the defined conventions
            final Class<?> msgType = message.getClass();
            final String msgKey = null != key ? key : new StringMessageKey(this.srcTopic, msgType.getName()).generateKey();

            //check the pre-configured filters if the message is to be discarded
            if (!isFiltered(dstTopic, msgType)) {
                kafkaSenders.get(dstTopic).send(msgKey, message);
            }
        }
    }

    /**
     * Checks if some message is not to be sent due to an existing pre configured filter
     *
     * @param dstTopic the destination topic to send the message into
     * @param msgType  the protobuf message type
     * @return {@code true} if the message is to be filtered, {@code true} otherwise
     */
    private boolean isFiltered(String dstTopic, Class<?> msgType) {
        for (final MessageFilter filter : filters) {
            if (filter.isEnabled() && filter.filter(dstTopic, msgType)) {
                final String filterName = filter.getClass().getSimpleName();
                log.debug("message {} is not going to be sent to {} due to filter: {}", msgType, dstTopic, filterName);
                return true;
            }
        }
        return false;
    }

    /**
     * Stop the message producers and its kafka senders
     */
    final void close() {
        log.info("Stopping all kafka producers..");
        this.kafkaSenders.values().forEach(KafkaSender::stop);
    }

}
