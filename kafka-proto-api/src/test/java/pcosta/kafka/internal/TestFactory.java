package pcosta.kafka.internal;

import com.google.protobuf.Any;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Message;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pcosta.kafka.api.MessageFilter;
import pcosta.kafka.api.MessageKey;
import pcosta.kafka.api.MessageListener;
import pcosta.kafka.api.MessageMetadata;
import pcosta.kafka.core.TestProto.SomeExtension;
import pcosta.kafka.core.TestProto.SomeOtherTestMessage;
import pcosta.kafka.core.TestProto.TestMessage;
import pcosta.kafka.internal.MessageReceiver.MessageProcessor;
import pcosta.kafka.message.KafkaMessageProto.KafkaMessage;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static com.google.protobuf.ExtensionRegistry.getEmptyRegistry;
import static org.junit.Assert.*;
import static pcosta.kafka.api.MessageListener.LATEST_OFFSET;

/**
 * @author Pedro Costa
 * <p/>
 * Factory for dummy testing Objects
 */
class TestFactory {

    /**
     * The test Message Processor
     */
    static class TestsMessageProcessor extends MessageProcessor {
        private static final Logger log = LoggerFactory.getLogger(TestsMessageProcessor.class);

        // the latch to be counted down when a message arrives
        private final CountDownLatch countDownLatch;

        /**
         * Notifies the messages listener is called: expecting cool messages
         */
        TestsMessageProcessor(String topic,
                              Class<?> messageType,
                              Collection<MessageFilter> filters,
                              Collection<MessageListener> listeners,
                              CountDownLatch latch) {

            super(topic, LATEST_OFFSET, 0, messageType, filters, listeners, getEmptyRegistry(),
                    error -> System.err.println("Received Error: " + error.toString()));
            this.countDownLatch = latch;
        }

        /**
         * Notifies the error listener is called: expecting errors
         */
        TestsMessageProcessor(String topic,
                              Class<?> messageType,
                              Collection<MessageFilter> filters,
                              Collection<MessageListener> listeners,
                              CountDownLatch latch,
                              CountDownLatch errorLatch) {

            super(topic, LATEST_OFFSET, 0, messageType, filters, listeners, getEmptyRegistry(), error -> {
                System.err.println("Received Error: " + error.getErrorDescription());
                errorLatch.countDown();
            });

            this.countDownLatch = latch;
        }

        @Override
        void process(final byte[] message, final String srcTopic, final MessageKey key, long offset) {
            log.info(">>> TestsMessageProcessor - Received a message from {} - offset: {}", srcTopic, offset);
            countDownLatch.countDown();
        }
    }

    /**
     * Builds a default proto KafkaMessage
     *
     * @param srcTopic the message src topic
     * @return the built KafkaMessage
     */
    static Message getDefaultMessage(String srcTopic) {
        return KafkaMessage.newBuilder()
                .setTraceabilityId("TraceabilityId")
                .setPayloadClass(TestMessage.class.getName())
                .setOriginTopic(srcTopic.toUpperCase())
                .setPayload(Any.pack(TestMessage.newBuilder().setText("testMessage").build()))
                .build();
    }

    /**
     * Builds a default proto KafkaMessage packing a {@link SomeOtherTestMessage}
     *
     * @param srcTopic the message src topic
     * @return the built KafkaMessage
     */
    static Message getSomeOtherDefaultMessage(String srcTopic) {
        return KafkaMessage.newBuilder()
                .setTraceabilityId("TraceabilityId")
                .setOriginTopic(srcTopic)
                .setPayloadClass(SomeOtherTestMessage.class.getName())
                .setPayload(Any.pack(SomeOtherTestMessage.newBuilder().setSomeText("SomeOtherTestMessage").build()))
                .build();
    }

    /**
     * Builds an proto KafkaMessage packing a {@link TestMessage} with an Extension of the type {@link SomeExtension}
     *
     * @param srcTopic the message src topic
     * @return the built KafkaMessage
     */
    static Message getDefaultMessageWithExtension(String srcTopic) {
        final TestMessage testMessage = TestMessage.newBuilder()
                .setText("testMessage")
                .setExtension(SomeExtension.element, SomeExtension.newBuilder().setInfo("100").build())
                .build();

        return KafkaMessage.newBuilder()
                .setTraceabilityId("TraceabilityId")
                .setPayloadClass(TestMessage.class.getName())
                .setOriginTopic(srcTopic)
                .setPayload(Any.pack(testMessage))
                .build();
    }

    // the test message registry for SomeExtensions
    static class SomeMessageRegistry {
        private static final ExtensionRegistry EXTENSION_REGISTRY = ExtensionRegistry.newInstance();

        private SomeMessageRegistry() {
        }

        static ExtensionRegistry getExtensionRegistry() {
            return EXTENSION_REGISTRY;
        }

        static {
            EXTENSION_REGISTRY.add(SomeExtension.element);
        }
    }

    // the test message listeners
    static class GenericMessageListener implements MessageListener<Message> {
        static final Logger log = LoggerFactory.getLogger(GenericMessageListener.class);
        // the latch to be counted down when a message arrives
        CountDownLatch latch;

        GenericMessageListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onMessage(MessageMetadata metadata, final Message message) {
            log.info(">>> GenericMessageListener -  Received: {}", message.toString());
            latch.countDown();
        }
    }

    static class KafkaMessageListener implements MessageListener<KafkaMessage> {
        static final Logger log = LoggerFactory.getLogger(KafkaMessageListener.class);
        // the latch to be counted down when a message arrives
        CountDownLatch latch;

        KafkaMessageListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onMessage(MessageMetadata metadata, final KafkaMessage message) {
            log.info(">>> KafkaMessageListener -  Received: {}", message.toString());
            latch.countDown();
        }
    }

    static class SomeMessageListener implements MessageListener<TestMessage> {
        static final Logger log = LoggerFactory.getLogger(SomeMessageListener.class);
        // the latch to be counted down when a message arrives
        CountDownLatch latch;

        SomeMessageListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onMessage(MessageMetadata metadata, final TestMessage message) {
            log.info(">>> SomeMessageListener -  Received: {}", message.toString());
            latch.countDown();
        }
    }

    // the test extension message listener
    static class SomeExtensionListener extends SomeMessageListener {
        SomeExtensionListener(CountDownLatch latch) {
            super(latch);
        }

        @Override
        public void onMessage(MessageMetadata metadata, final TestMessage message) {
            log.info(">>> SomeExtensionListener -  Received: {}", message.toString());

            // assert all the extension fields were correctly deserialized and there's no unknown field left
            assertTrue(message.hasExtension(SomeExtension.element));
            assertTrue(message.getExtension(SomeExtension.element).hasInfo());
            assertFalse(message.getUnknownFields().hasField(0));

            // assert metadata
            assertFalse(metadata.getTraceabilityId().isEmpty());
            assertNotNull(metadata.getKey());
            assertNotNull(metadata.getSrcTopic());
            latch.countDown();
        }
    }

    /**
     * @return the kafka sender test properties
     */
    static Map<String, Object> senderProps(String port) {
        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + port);
        props.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 3000);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 3000);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProtobufSerializer.class);
        return props;
    }

    /**
     * @return the kafka receiver test properties
     */
    static Properties receiverProps(String port) {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + port);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group");
        properties.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, 3000);
        properties.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return properties;
    }

    // the test message filters
    static class SomeMessageFilter implements MessageFilter {

        @Override
        public boolean filter(String topic, Class<?> msgType) {
            return true;
        }

        @Override
        public boolean isEnabled() {
            return true;
        }

        @Override
        public void enable() {
            //noop
        }

        @Override
        public void disable() {
            //noop
        }
    }

    @SuppressWarnings("unused")
    public static class SomeProtoMessage extends GeneratedMessage {

        @Override
        protected FieldAccessorTable internalGetFieldAccessorTable() {
            return null;
        }

        @Override
        protected Message.Builder newBuilderForType(BuilderParent parent) {
            return null;
        }

        @Override
        public Message.Builder newBuilderForType() {
            return null;
        }

        @Override
        public Message.Builder toBuilder() {
            return null;
        }

        @Override
        public Message getDefaultInstanceForType() {
            return null;
        }
    }

}
