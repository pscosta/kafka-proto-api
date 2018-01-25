package pcosta.kafka.internal;

import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pcosta.kafka.api.*;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The messaging context implementation
 *
 * @author Pedro Costa
 */
@SuppressWarnings("unchecked")
class KafkaContext implements MessagingContext {

    // the logger
    private static final Logger log = LoggerFactory.getLogger(KafkaContext.class);

    // the receiver reference
    private MessageReceiver receiver;
    // the producer reference
    private Map<String, MessageProducerImpl> producers;

    /**
     * Default context constructor.
     */
    KafkaContext() {
        this.producers = new ConcurrentHashMap<>();
    }

    @Override
    public void createReceiver(final MessageReceiverConfiguration configuration) throws MessagingException {
        if (receiver != null) {
            throw new IllegalStateException("Receiver has been created already");
        }

        // create the receiver and register the platform error listener
        this.receiver = new MessageReceiver();
        // register the error listener
        receiver.registerErrorListener(configuration.getErrorListener());
        // register the message listeners
        configuration.getListeners().forEach(listenerConfiguration -> receiver.registerListener(listenerConfiguration));
        // start the receiver and all its underling kafka consumers
        initializeMessageReceiver();
    }

    @Override
    public <M> MessageProducer<M> createProducer(final String key,
                                                 final Serializer keySerializer,
                                                 final Serializer valueSerializer) throws MessagingException {

        final MessageProducerImpl<M> newProducer = new MessageProducerImpl<>(keySerializer, valueSerializer);
        final MessageProducerImpl<M> oldProducer = producers.putIfAbsent(key, newProducer);
        if (Objects.isNull(oldProducer)) {
            return newProducer;
        } else {
            return oldProducer;
        }
    }

    @Override
    public <M> MessageProducer<M> createProducer(final String key,
                                                 final Serializer keySerializer,
                                                 final Serializer valueSerializer,
                                                 final Collection<MessageFilter> filters) throws MessagingException {

        final MessageProducerImpl<M> newProducer = new MessageProducerImpl<>(keySerializer, valueSerializer, filters);
        final MessageProducerImpl<M> oldProducer = producers.putIfAbsent(key, newProducer);
        if (Objects.isNull(oldProducer)) {
            return newProducer;
        } else {
            return oldProducer;
        }
    }

    @Override
    public void shutdown() throws MessagingException {
        log.info("kafka context is shutting down..");
        // cleanup the registered receivers
        if (receiver != null) {
            this.receiver.close();
            this.receiver = null;
        }
        // cleanup the registered producers
        producers.forEach((key, messageProducer) -> messageProducer.close());
    }

    /**
     * Initializes the platform message receiver
     */
    private void initializeMessageReceiver() {
        receiver.start();
    }

}
