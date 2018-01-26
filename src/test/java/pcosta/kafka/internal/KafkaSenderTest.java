package pcosta.kafka.internal;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.util.concurrent.ListenableFuture;
import pcosta.kafka.core.TestProto.TestMessage;

import java.util.concurrent.CountDownLatch;

import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static pcosta.kafka.internal.TestFactory.receiverProps;
import static pcosta.kafka.internal.TestFactory.senderProps;

/**
 * @author Pedro Costa
 * <p>
 * Unit tests for the {@link KafkaSenderTest} class
 */
@SuppressWarnings("unchecked")
public class KafkaSenderTest {

    // object under testing
    private KafkaSender<String, byte[]> kafkaSender;

    // the testing topics
    private static final String RECEIVER_TOPIC = "Topic";
    private static final String SENDER_TOPIC = "Topic2";

    // the default proto message
    private final TestMessage DEFAULT_MSG = TestMessage.newBuilder().setText("testMessage").build();
    // the default message key
    private final String DEFAULT_KEY = SENDER_TOPIC + "|" + TestMessage.class.getName();
    // the broker dynamic listening port
    private String port;

    /**
     * Creates the embedded Kafka broker for some receiver Topic
     * <p>
     * param count the number of brokers.
     * param controlledShutdown passed into TestUtils.createBrokerConfig.
     * param partitions partitions per topic.
     * param topics the topics to create.
     * <p>
     * public KafkaEmbedded(int count, boolean controlledShutdown, int partitions, String... topics);
     */
    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, 1, SENDER_TOPIC, RECEIVER_TOPIC);

    @Before
    public void setUp() {
        //Prepare: Wait for the kafka broker to be in sync
        System.setProperty(PropertiesReader.CONFIGURATION_FILE_DIR, "src/test/resources");
        embeddedKafka.waitUntilSynced(RECEIVER_TOPIC, 0);
        this.port = embeddedKafka.getKafkaServer(0).config().port().toString();
    }

    @After
    public void destroy() {
        // unregister and destroy the receiver
        if (kafkaSender != null) {
            this.kafkaSender.stop();
        }
    }

    @Test
    public void send_success() throws Exception {
        //Prepare
        final CountDownLatch latch = new CountDownLatch(1);
        this.kafkaSender = new KafkaSender<>(RECEIVER_TOPIC, new StringSerializer(), new ByteArraySerializer(), senderProps(port));

        //create and start the kafka receiver
        final KafkaReceiver receiver = this.createAndStartReceiver(latch);

        // Call: send the message
        final ListenableFuture<SendResult<String, byte[]>> sendResult = kafkaSender.send(DEFAULT_KEY, DEFAULT_MSG.toByteArray());

        //Assert
        assertTrue(latch.await(10, SECONDS));
        assertTrue(sendResult.get().getProducerRecord() != null);

        // stop the receiver
        kafkaSender.stop();
        receiver.stop();
    }

    @Test
    public void send_to_invalid_topic() throws Exception {
        //Prepare
        final CountDownLatch latch = new CountDownLatch(1);
        this.kafkaSender = new KafkaSender<>(SENDER_TOPIC, new StringSerializer(), new ByteArraySerializer(), senderProps(port));

        //create and start the kafka receiver
        final KafkaReceiver receiver = this.createAndStartReceiver(latch);

        // Call: send the message
        final ListenableFuture<SendResult<String, byte[]>> sendResult = kafkaSender.send(DEFAULT_KEY, DEFAULT_MSG.toByteArray());

        //Assert
        assertFalse(latch.await(5, SECONDS));
        assertTrue(sendResult.get().getProducerRecord() != null);

        // stop the receiver
        kafkaSender.stop();
        receiver.stop();
    }

    /**
     * Creates the Kafka test receiver
     */
    private KafkaReceiver createAndStartReceiver(CountDownLatch latch) throws InterruptedException {
        final KafkaReceiver receiver = new KafkaReceiver(
                RECEIVER_TOPIC,
                new StringDeserializer(),
                new ByteArrayDeserializer(),
                new TestFactory.TestsMessageProcessor(RECEIVER_TOPIC, TestMessage.class, emptyList(), emptyList(), latch), 0, // latest offset and partition = 0
                receiverProps(port));

        //Wait some seconds for it be properly registered on broker
        Thread.sleep(6000);
        receiver.start();
        return receiver;
    }
}