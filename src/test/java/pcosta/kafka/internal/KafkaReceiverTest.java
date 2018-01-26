package pcosta.kafka.internal;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import pcosta.kafka.core.TestProto.TestMessage;
import pcosta.kafka.internal.MessageReceiver.MessageProcessor;
import pcosta.kafka.internal.TestFactory.TestsMessageProcessor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static java.lang.Thread.sleep;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;
import static pcosta.kafka.internal.TestFactory.receiverProps;
import static pcosta.kafka.internal.TestFactory.senderProps;

/**
 * @author Pedro Costa
 * <p>
 * Unit tests for the {@link KafkaReceiver} class
 */
@SuppressWarnings("unchecked")
public class KafkaReceiverTest {

    // number of messages to be sent to the Kafka Receiver
    private static int messagesToBeSent = 2;

    // static test fields
    private static final TestMessage DEFAULT_MESSAGE = TestMessage.newBuilder().setText("testMessage").build();

    // waiting for messages to be delivered timeout
    private static final int MESSAGE_TIMEOUT = 10;
    private static final int PARTITION = 0;
    private static final long LATEST_OFFSET = -1L;

    // kafka sender registering wait time
    private static final int REGISTER_WAIT_TIME = 6000;
    private static final String DEFAULT_KEY = "Topic" + "|" + TestMessage.class.getName();
    private static final String DST_TOPIC = "Topic2";

    // key and value deserializers
    private static final StringDeserializer KEY_DESERIALIZER = new StringDeserializer();
    private static final ByteArrayDeserializer VAL_DESERIALIZER = new ByteArrayDeserializer();

    // object under testing
    private KafkaReceiver<String, byte[]> kafkaReceiver;
    // the test message processor
    private MessageProcessor msgProcessor;
    //the default senders factory
    private DefaultKafkaProducerFactory<String, TestMessage> senderFactory;
    // the broker dynamic listening port
    private String port;

    /**
     * Creates the embedded Kafka broker for Some receiver Topic
     * <p>
     * param count the number of brokers.
     * param controlledShutdown passed into TestUtils.createBrokerConfig.
     * param partitions partitions per topic.
     * param topics the topics to create.
     * <p>
     * public KafkaEmbedded(int count, boolean controlledShutdown, int partitions, String... topics);
     */
    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, 1, DST_TOPIC);

    @Before
    public void setUp() {
        // wait until the broker is ready
        System.setProperty(PropertiesReader.CONFIGURATION_FILE_DIR, "src/test/resources");
        embeddedKafka.waitUntilSynced(DST_TOPIC, 0);
        this.port = embeddedKafka.getKafkaServer(0).config().port().toString();
    }

    @After
    public void destroy() throws Exception {
        // unregister and destroy the receiver
        if (kafkaReceiver != null) {
            this.kafkaReceiver.stop();
        }
        if (this.senderFactory != null) {
            this.senderFactory.destroy();
        }
    }

    @Test
    public void sendSuccessfulMessage() throws ExecutionException, InterruptedException {
        // Prepare
        final CountDownLatch latch = new CountDownLatch(messagesToBeSent);
        this.msgProcessor = new TestsMessageProcessor(DST_TOPIC, TestMessage.class, emptyList(), emptyList(), latch);

        // create and start the kafka receiver and Sender
        this.kafkaReceiver = new KafkaReceiver(DST_TOPIC, KEY_DESERIALIZER, VAL_DESERIALIZER, msgProcessor, PARTITION, receiverProps(port));
        this.kafkaReceiver.start();
        final KafkaTemplate<String, TestMessage> kafkaSender = createTemplate(DST_TOPIC);

        // Call: send the messages to the broker
        for (int i = 0; i < messagesToBeSent; ++i) {
            final SendResult<String, TestMessage> sendResult = kafkaSender.sendDefault(DEFAULT_KEY, DEFAULT_MESSAGE).get();
            // Assert message was sent
            assertTrue(sendResult.getProducerRecord().value() != null);
        }

        // Assert: message was received and the latch was counted down on msgListener #messagesToBeSent times
        assertTrue(latch.await(MESSAGE_TIMEOUT, SECONDS));
        kafkaReceiver.stop();
    }

    @Test
    public void send_unknown_Key_format() throws Exception {
        // Prepare
        final CountDownLatch latch = new CountDownLatch(messagesToBeSent);
        final CountDownLatch errorLatch = new CountDownLatch(messagesToBeSent);
        this.msgProcessor = new TestsMessageProcessor(DST_TOPIC, TestMessage.class, emptyList(), emptyList(), latch, errorLatch);

        // create and start the kafka receiver and Sender
        this.kafkaReceiver = new KafkaReceiver(DST_TOPIC, KEY_DESERIALIZER, VAL_DESERIALIZER, msgProcessor, PARTITION, receiverProps(port));
        this.kafkaReceiver.start();
        final KafkaTemplate<String, TestMessage> kafkaSender = createTemplate(DST_TOPIC);

        // Call: send the messages to the broker
        for (int i = 0; i < messagesToBeSent; ++i) {
            kafkaSender.sendDefault("UnknownKey", DEFAULT_MESSAGE).get();
        }

        // Assert: message was received and the latch was counted down on msgListener #messagesToBeSent times
        assertTrue(latch.await(1, SECONDS));
        kafkaReceiver.stop();
    }

    /**
     * creates a kafka sender template
     *
     * @param dstTopic the destination topic
     */
    private KafkaTemplate<String, TestMessage> createTemplate(String dstTopic) throws InterruptedException {
        this.senderFactory = new DefaultKafkaProducerFactory<>(senderProps(port));
        final KafkaTemplate<String, TestMessage> template = new KafkaTemplate<>(senderFactory, true);
        template.setDefaultTopic(dstTopic);
        sleep(REGISTER_WAIT_TIME);
        return template;
    }
}
