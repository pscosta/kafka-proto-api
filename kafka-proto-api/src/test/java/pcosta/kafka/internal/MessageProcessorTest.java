package pcosta.kafka.internal;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import pcosta.kafka.api.MessageFilter;
import pcosta.kafka.api.MessageKey;
import pcosta.kafka.api.MessageListener;
import pcosta.kafka.api.PlatformErrorListener;
import pcosta.kafka.core.TestProto.SomeOtherTestMessage;
import pcosta.kafka.core.TestProto.TestMessage;
import pcosta.kafka.internal.MessageReceiver.MessageProcessor;
import pcosta.kafka.message.KafkaMessageProto.KafkaMessage;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.google.protobuf.ExtensionRegistry.getEmptyRegistry;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static pcosta.kafka.api.MessageListener.LATEST_OFFSET;
import static pcosta.kafka.internal.TestFactory.*;

/**
 * @author Pedro Costa
 * <p/>
 * Unit test for {@link MessageProcessor} class
 */
@SuppressWarnings("unchecked")
public class MessageProcessorTest {

    private static final int DEFAULT_TIMEOUT = 2;
    private static final long DEFAULT_OFFSET = 1L;

    // the testing topics
    private static final String RECEIVER_TOPIC = "Topic1";
    private static final String SENDER_TOPIC = "Topic2";

    // the message keys
    private static final MessageKey DEFAULT_KEY = new StringMessageKey(SENDER_TOPIC + "|" + TestMessage.class.getName());
    private static final MessageKey UNKNOWN_TYPE_KEY = new StringMessageKey(SENDER_TOPIC + "|" + SomeOtherTestMessage.class.getName());

    // object under testing
    private MessageProcessor messageProcessor;

    /**
     * Creates the embedded Kafka broker for the {@code RECEIVER_TOPIC}
     * <p>
     * param count the number of brokers.
     * param controlledShutdown passed into TestUtils.createBrokerConfig.
     * param partitions partitions per topic.
     * param topics the topics to create.
     * <p>
     * public KafkaEmbedded(int count, boolean controlledShutdown, int partitions, String... topics);
     */
    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, 1, RECEIVER_TOPIC);

    @Before
    public void setUp() {
        // wait until the broker is ready
        System.setProperty(PropertiesReader.CONFIGURATION_FILE_DIR, "src/test/resources");
        embeddedKafka.waitUntilSynced(RECEIVER_TOPIC, 0);
        final String port = embeddedKafka.getKafkaServer(0).config().port().toString();
        System.setProperty("spring.embedded.kafka.brokers", "localhost:" + port);
    }

    @Test
    public void processMessage_success() throws Exception {
        //Prepare
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch errorLatch = new CountDownLatch(1);
        final List<MessageListener> messageListeners = Collections.singletonList(new TestFactory.SomeMessageListener(latch));
        final List<MessageFilter> messageFilters = Collections.emptyList();

        //init the processor
        this.messageProcessor = new MessageProcessor(
                RECEIVER_TOPIC, LATEST_OFFSET, 0,
                TestMessage.class, messageFilters, messageListeners, getEmptyRegistry(),
                getErrorListener(errorLatch));

        //Call
        messageProcessor.process(getDefaultMessage(SENDER_TOPIC).toByteArray(), SENDER_TOPIC, DEFAULT_KEY, DEFAULT_OFFSET);

        //Assert: the listener was called back
        assertTrue(latch.await(DEFAULT_TIMEOUT, SECONDS));
        messageProcessor.stopReceiver();
    }

    @Test
    public void processMessage_KafkaMessage_success() throws Exception {
        //Prepare
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch errorLatch = new CountDownLatch(1);
        final List<MessageListener> messageListeners = Collections.singletonList(new TestFactory.KafkaMessageListener(latch));
        final List<MessageFilter> messageFilters = Collections.emptyList();

        //init the processor
        this.messageProcessor = new MessageProcessor(
                RECEIVER_TOPIC, LATEST_OFFSET, 0,
                KafkaMessage.class, messageFilters, messageListeners, getEmptyRegistry(),
                getErrorListener(errorLatch));

        //Call
        messageProcessor.process(getDefaultMessage(SENDER_TOPIC).toByteArray(), SENDER_TOPIC, DEFAULT_KEY, DEFAULT_OFFSET);

        //Assert: the listener was called back
        assertTrue(latch.await(DEFAULT_TIMEOUT, SECONDS));
        messageProcessor.stopReceiver();
    }

    @Test
    public void processMessage_generic_message_successV3() throws Exception {
        //Prepare
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch errorLatch = new CountDownLatch(1);
        final List<MessageListener> messageListeners = Collections.singletonList(new TestFactory.GenericMessageListener(latch));
        final List<MessageFilter> messageFilters = Collections.emptyList();

        //init the processor
        this.messageProcessor = new MessageProcessor(
                RECEIVER_TOPIC, LATEST_OFFSET, 0,
                KafkaMessage.class, messageFilters, messageListeners, getEmptyRegistry(),
                getErrorListener(errorLatch));

        //Call
        messageProcessor.process(getDefaultMessage(SENDER_TOPIC).toByteArray(), SENDER_TOPIC, DEFAULT_KEY, DEFAULT_OFFSET);

        //Assert: the listener was called back
        assertTrue(latch.await(DEFAULT_TIMEOUT, SECONDS));
        messageProcessor.stopReceiver();
    }

    @Test
    public void processMessage_filteredMessage() throws Exception {
        //Prepare
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch errorLatch = new CountDownLatch(1);
        final List<MessageListener> messageListeners = Collections.singletonList(new TestFactory.SomeMessageListener(latch));
        final List<MessageFilter> messageFilters = Collections.singletonList(new TestFactory.SomeMessageFilter());

        //init the processor
        this.messageProcessor = new MessageProcessor(
                RECEIVER_TOPIC, LATEST_OFFSET, 0,
                TestMessage.class, messageFilters, messageListeners, getEmptyRegistry(),
                getErrorListener(errorLatch));

        //Call
        messageProcessor.process(getDefaultMessage(SENDER_TOPIC).toByteArray(), SENDER_TOPIC, DEFAULT_KEY, DEFAULT_OFFSET);

        //Assert: the listener wasn't called back because the message was filtered
        assertFalse(latch.await(DEFAULT_TIMEOUT, SECONDS));
        messageProcessor.stopReceiver();
    }

    @Test
    public void processMessage_UnknownMessageType() throws Exception {
        //Prepare
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch errorLatch = new CountDownLatch(1);
        final List<MessageListener> messageListeners = Collections.emptyList(); // no listeners for known messages
        final List<MessageFilter> messageFilters = Collections.emptyList();

        //init the processor
        this.messageProcessor = new MessageProcessor(
                RECEIVER_TOPIC, LATEST_OFFSET, 0,
                Object.class, messageFilters, messageListeners, getEmptyRegistry(),
                getErrorListener(errorLatch));

        //Call
        messageProcessor.process(getDefaultMessage(SENDER_TOPIC).toByteArray(), SENDER_TOPIC, DEFAULT_KEY, DEFAULT_OFFSET);

        //Assert: the error listener was called back because the message type is unknown
        assertFalse(latch.await(DEFAULT_TIMEOUT, SECONDS));
        assertTrue(errorLatch.await(DEFAULT_TIMEOUT, SECONDS));
        messageProcessor.stopReceiver();
    }

    @Test
    public void processMessage_Extensions() throws Exception {
        //Prepare
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch errorLatch = new CountDownLatch(1);
        final List<MessageListener> listeners = Collections.singletonList(new TestFactory.SomeExtensionListener(latch));
        final List<MessageFilter> filters = Collections.emptyList();

        //init the processor
        this.messageProcessor = new MessageProcessor(
                RECEIVER_TOPIC, LATEST_OFFSET, 0,
                TestMessage.class, filters, listeners, TestFactory.SomeMessageRegistry.getExtensionRegistry(),
                getErrorListener(errorLatch));

        //Call
        messageProcessor.process(getDefaultMessageWithExtension(SENDER_TOPIC).toByteArray(), SENDER_TOPIC, DEFAULT_KEY, DEFAULT_OFFSET);

        //Assert: the listener wasn't called back because the message was filtered
        assertTrue(latch.await(DEFAULT_TIMEOUT, SECONDS));
        messageProcessor.stopReceiver();
    }

    @Test
    public void processMessage_UnknownType() throws Exception {
        //Prepare
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch errorLatch = new CountDownLatch(1);
        final List<MessageListener> listeners = Collections.singletonList(new TestFactory.SomeMessageListener(latch));
        final List<MessageFilter> filters = Collections.singletonList(new TestFactory.SomeMessageFilter());

        //init the processor
        this.messageProcessor = new MessageProcessor(
                RECEIVER_TOPIC, LATEST_OFFSET, 0,
                TestMessage.class, filters, listeners, getEmptyRegistry(),
                getErrorListener(errorLatch));

        //Call
        messageProcessor.process(getSomeOtherDefaultMessage(SENDER_TOPIC).toByteArray(), SENDER_TOPIC, UNKNOWN_TYPE_KEY, DEFAULT_OFFSET);

        //Assert: the listener wasn't called back because no listener was registered for this message type
        assertFalse(latch.await(DEFAULT_TIMEOUT, SECONDS));
        assertTrue(errorLatch.await(DEFAULT_TIMEOUT, SECONDS));
        messageProcessor.stopReceiver();
    }

    /**
     * produces the test error listener
     *
     * @return the error listener
     */
    private PlatformErrorListener getErrorListener(CountDownLatch latch) {
        return error -> {
            latch.countDown();
            System.err.println("Received error: " + error.getErrorDescription());
        };
    }

}