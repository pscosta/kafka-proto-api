package pcosta.kafka.internal;

import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pcosta.kafka.api.*;
import pcosta.kafka.message.KafkaMessageProto.KafkaMessage;

import java.util.*;

import static com.google.protobuf.ExtensionRegistry.getEmptyRegistry;
import static java.lang.System.lineSeparator;
import static java.util.Collections.emptyList;
import static pcosta.kafka.internal.MessageReceiver.ProtoBufType.DEFAULT_PROTO_TYPE;

/**
 * @author Pedro Costa
 * <p/>
 * Responsible for deSerializing and dispatching received protobuf messages form kafka broker
 */
class MessageReceiver {

    private static final Logger log = LoggerFactory.getLogger(MessageReceiver.class);

    // the message listenersMap by topic
    private final Map<String, MessageProcessor> processors;

    // the error listener
    private PlatformErrorListener errorListener;

    MessageReceiver() {
        this.processors = new HashMap<>();
    }

    /**
     * Registers a message listener with the specified configuration
     *
     * @param config the listener configuration
     */
    final <M extends Message> void registerListener(final MessageListenerConfiguration<M> config) {

        config.getTopics().forEach(topic -> {
            //create new processor for this topic and register the listenersMap for the defined messages
            if (!processors.containsKey(topic)) {
                processors.putIfAbsent(topic, new MessageProcessor(
                        topic,
                        config.getOffset(),
                        config.getPartition(),
                        config.getMessageType(),
                        config.getMessageFilters(),
                        config.getMessageListeners(),
                        config.getExtensionRegistry(),
                        errorListener));
            }
            // use the existing processor and register the listenersMap for the defined messages
            else {
                final MessageProcessor processor = processors.get(topic);
                processor.registerListeners(config.getMessageType(), config.getMessageListeners(), config.getExtensionRegistry());
                // hack to bypass several listeners for the same topic w/ different offsets -> the largest wins
                if (config.getOffset() > processor.initialOffset) processor.initialOffset = config.getOffset();
            }
            log.info("A listener for {} type was registered for topic: {}", config.getMessageType(), topic);
        });
    }

    /**
     * Registers a {@link PlatformErrorListener} that will handle the processing errors
     *
     * @param errorListener the error listener
     */
    final void registerErrorListener(PlatformErrorListener errorListener) {
        this.errorListener = errorListener;
    }

    /**
     * Start the message processors and its kafka receivers
     */
    final void start() {
        log.info("Starting the kafka message receivers");
        processors.forEach((topic, messageProcessor) -> messageProcessor.startReceiver());
    }

    /**
     * Stop the message processors and its kafka receivers
     */
    final void close() {
        log.info("Stopping all kafka listeners..");
        processors.forEach((topic, messageProcessor) -> messageProcessor.stopReceiver());
    }

    /**
     * The message processor for proto messages incoming from a given {@code Topic}
     */
    @SuppressWarnings("unchecked")
    static class MessageProcessor {
        // the supported proto message types
        private final Map<String, ProtoBufType> supportedTypes;
        // the proto message listenersMap
        private final Map<Class<?>, Collection<MessageListener>> listenersMap;
        // the byte[] to protobuf message deserializer
        private final ProtobufDeserializer protoDeserializer;
        // the message filters
        private final Map<String, Collection<MessageFilter>> filtersMap;

        // the incoming messages topic info
        private final String topic;
        final int partition;
        long initialOffset;

        // the kafka receiver
        private final KafkaReceiver<String, byte[]> kafkaReceiver;

        // the error listener
        private PlatformErrorListener errorListener;

        MessageProcessor(String topic,
                         long offset, int partition,
                         Class<?> messageType,
                         Collection<MessageFilter> filters,
                         Collection<MessageListener> listeners,
                         ExtensionRegistry registry,
                         PlatformErrorListener errorListener) {

            this.topic = topic;
            this.initialOffset = offset;
            this.partition = partition;
            this.errorListener = errorListener;
            this.listenersMap = new HashMap<>();
            this.supportedTypes = new HashMap<>();
            this.filtersMap = new HashMap<>();
            this.protoDeserializer = new ProtobufDeserializer();

            filtersMap.put(messageType.getName(), filters);
            supportedTypes.put(messageType.getName(), new ProtoBufType(messageType, registry));
            listenersMap.put(messageType, listeners);

            //create the concrete kafka receiver for String key types and byte[] message values
            this.kafkaReceiver = new KafkaReceiver(this.topic, new StringDeserializer(), new ByteArrayDeserializer(), this);
        }

        /**
         * Processes the received {@link Message}, deserialize its payload into proto and deliver it to the registered listeners
         *
         * @param KafkaMessageBytes the received {@link Message} in bytes
         * @param srcTopic          the kafka topic from which the message was received
         * @param key               the received kafka message key
         */
        void process(byte[] KafkaMessageBytes, String srcTopic, MessageKey key, long offset) {
            try {
                // parse the received KafkaMessage bytes into a protobuf type
                final Message payload;
                final KafkaMessage kafkaMsg = KafkaMessage.parseFrom(KafkaMessageBytes);

                // check if there's a listener registered for all incoming messages from this Topic
                final ProtoBufType defaultType = supportedTypes.get(KafkaMessage.class.getName()) == null ? null : DEFAULT_PROTO_TYPE;
                final ProtoBufType protoType = supportedTypes.getOrDefault(kafkaMsg.getPayloadClass(), defaultType);

                if (Objects.isNull(protoType)) {
                    final String error = String.format("Received unsupported payload. Source: %s , Type: %s", srcTopic, kafkaMsg);
                    log.warn(error);
                    errorListener.onError(new PlatformErrorImpl(error, new MessagingException(error)));
                    return;
                }

                //check if the message is to be discarded due to a pre-configured filter
                if (isFiltered(srcTopic, protoType.messageType)) return;

                // if there's a listener registered for KafkaMessages, let us deliver it without further parsing
                if (protoType == DEFAULT_PROTO_TYPE) {
                    payload = kafkaMsg;
                }
                //parse the KafkaMessage payload bytes into the payload protobuf type
                else payload = protoDeserializer.parseFromV3(kafkaMsg, protoType.extensionRegistry);

                if (log.isDebugEnabled()) {
                    log.debug("message has been received:{}" + "CorrelationId: {}{}" + "source topic: {}{}" + "payload: {}{}",
                            lineSeparator(), srcTopic, lineSeparator(), payload.toString(), lineSeparator()
                    );
                }

                //deliver the message to its registered listenersMap
                listenersMap.get(protoType.messageType).forEach(listener -> {
                    log.debug("delivering message to {}", listener.getClass().getSimpleName());
                    listener.onMessage(new KafkaMetadata(srcTopic, key, offset, kafkaMsg.getTraceabilityId()), payload);
                });
            } catch (Exception e) {
                errorListener.onError(new PlatformErrorImpl(e.getClass().getName(), e.getCause()));
                log.error("Error processing message: ", e);
            }
        }

        /**
         * Checks if the previously received message is indeed filtered (if any filters exists) and can be delivered.
         *
         * @param source  the source of the message
         * @param msgType the class of the message payload
         * @return {@code false} if there are no filters or they exist and the message is compliant with them, {@code true} otherwise.
         */
        private boolean isFiltered(final String source, final Class<?> msgType) {
            return filtersMap.getOrDefault(msgType.getName(), emptyList())
                    .stream()
                    .filter(filter -> filter.isEnabled() && filter.filter(source, msgType))
                    .peek(filter -> log.warn("message {} from {} is not going to be delivered to application due to filter: {}",
                            msgType, source, filter.getClass().getSimpleName()))
                    .findAny()
                    .isPresent();
        }

        /**
         * Registers the given listenersMap for proto messages incoming from this processor's topic
         *
         * @param messageType the protobuf message type
         * @param listeners   the message listenersMap
         * @param registry    the message type Extension Registry
         */
        private void registerListeners(Class<?> messageType, Collection<MessageListener> listeners, ExtensionRegistry registry) {
            this.listenersMap.computeIfAbsent(messageType, type -> new HashSet<>()).addAll(listeners);
            supportedTypes.putIfAbsent(messageType.getName(), new ProtoBufType(messageType, registry));
            log.info("A listener for {} type was registered for topic: {}", messageType, topic);
        }

        /**
         * Delivers the specified error to the platform listener.
         *
         * @param error the platform error to be delivered
         */
        void processError(final PlatformError error) {
            log.debug("delivering error to handler..");
            errorListener.onError(error);
        }

        /**
         * Start the kafka message receiver
         */
        void startReceiver() {
            this.kafkaReceiver.start();
        }

        /**
         * Stop the kafka message receiver
         */
        void stopReceiver() {
            this.kafkaReceiver.stop();
        }
    }

    /**
     * Links a Protobuf message Type with its respective {@link ExtensionRegistry}
     */
    static class ProtoBufType {
        static final ProtoBufType DEFAULT_PROTO_TYPE = new ProtoBufType(KafkaMessage.class, getEmptyRegistry());
        final Class<?> messageType;
        final ExtensionRegistry extensionRegistry;

        ProtoBufType(Class<?> messageType, ExtensionRegistry extensionRegistry) {
            this.messageType = messageType;
            this.extensionRegistry = extensionRegistry;
        }
    }

}
