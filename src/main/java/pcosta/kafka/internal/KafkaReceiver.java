package pcosta.kafka.internal;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pcosta.kafka.internal.MessageReceiver.MessageProcessor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.singletonList;
import static pcosta.kafka.api.MessageListener.*;

/**
 * @author Pedro Costa
 * <p>
 * Implementation of a Kafka protobuf messages receiver
 */
@SuppressWarnings("unchecked")
class KafkaReceiver<KEY, IN> {

    private static final Logger log = LoggerFactory.getLogger(KafkaReceiver.class);

    // record polling timeout
    private static final int POLL_TIMEOUT = 1000;

    // the kafka consumer and its thread
    private Thread consumerThread;
    private KafkaConsumer<KEY, IN> consumer;
    private static AtomicBoolean shuttingDown = new AtomicBoolean(false);

    private final String topic;
    private final Deserializer<KEY> keyDeserializer;
    private final Deserializer<IN> valueDeserializer;
    private final Map<String, Object> consumerProperties;
    private final int partition;

    // the message processor
    private final MessageProcessor delegate;

    /**
     * Default constructor
     *
     * @param topic             the topic assigned to this receiver
     * @param keyDeserializer   the message key Deserializer
     * @param valueDeserializer the message value Deserializer
     * @param delegate          the message processor to be used as callback for new incoming messages
     */
    KafkaReceiver(String topic, Deserializer<KEY> keyDeserializer, Deserializer<IN> valueDeserializer, MessageProcessor delegate) {
        this.topic = topic;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.delegate = delegate;
        this.partition = delegate.partition;

        // load the consumer properties
        this.consumerProperties = loadConsumerProps();
    }

    /**
     * @param topic             the topic assigned to this receiver
     * @param keyDeserializer   the message key {@link Deserializer}
     * @param valueDeserializer the protobuf message value {@link Deserializer}
     * @param delegate          the message processor to be used as callback for new incoming messages
     * @param properties        the consumer properties
     */
    KafkaReceiver(String topic,
                  Deserializer<KEY> keyDeserializer,
                  Deserializer<IN> valueDeserializer,
                  MessageProcessor delegate,
                  int partition,
                  Map<String, Object> properties) {

        this.topic = topic;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.delegate = delegate;
        this.partition = partition;

        // use the incoming properties
        this.consumerProperties = properties;
    }

    /**
     * Creates the Kafka consumer container
     */
    private void createConsumer() {
        log.info("Initiating Kafka Receiver for Topic: {} Partition: {} Initial Offset: {}", topic, partition, delegate.initialOffset);
        this.consumer = getKafkaConsumer();
        this.consumerThread = new Thread(this::pollRecords, topic + "KafkaConsumer");
    }

    /**
     * Checks the subscribed Topic for new kafka {@link ConsumerRecord}s
     */
    private void pollRecords() {
        try {
            while (true) {
                for (ConsumerRecord<KEY, IN> record : consumer.poll(POLL_TIMEOUT)) {
                    // call the delegate processor
                    try {
                        delegate.process((byte[]) record.value(), topic, new StringMessageKey(record.key()), record.offset());
                    } catch (IllegalArgumentException e) {
                        log.error("Impossible to deliver message to processor: {}", e.getMessage(), e);
                        delegate.processError(new PlatformErrorImpl(e.getMessage(), e.getCause()));
                    }
                }
                if (shuttingDown.get()) return;
            }
        } catch (Exception e) {
            log.error("SEVERE error pooling records: ", e);
        } finally {
            this.consumer.close();
        }
    }

    /**
     * @return a Kafka consumer for the subscribed {@code topic}
     */
    private KafkaConsumer<KEY, IN> getKafkaConsumer() {
        final long offset = delegate.initialOffset;
        try {
            final KafkaConsumer<KEY, IN> consumer = new KafkaConsumer<>(consumerProperties);

            //Assign the controller topic and 0 partition
            final TopicPartition topicPartition = new TopicPartition(topic, 0);
            final List<TopicPartition> partitions = singletonList(topicPartition);
            consumer.assign(partitions);

            // if the last committed offset is smaller than the one provided, we need to replay all records
            final OffsetAndMetadata committed = consumer.committed(topicPartition);
            if (offset != LATEST_OFFSET && (committed == null || committed.offset() < offset)) {
                log.warn("Provided offset: {} is ahead of the last committed one: {}. Assuming LATEST", offset, committed);
                delegate.processError(new PlatformErrorImpl("Invalid offset provided", new NoOffsetForPartitionException(topicPartition)));
                this.delegate.initialOffset = LATEST_OFFSET;
            }
            // Latest offset defined - only fetch new messages
            if (offset == LATEST_OFFSET) {
                //only consume recent messages
                consumer.seekToEnd(partitions);
                //initial poll for preemptive metadata and offsets negotiation
                consumer.poll(POLL_TIMEOUT);
            }
            // Latest offset defined - only fetch new messages
            else if (offset == EARLIEST_OFFSET) {
                //only consume recent messages
                consumer.seekToBeginning(partitions);
            }
            // if the provided offset is to be taken into account, let us seek to the desired position
            else if (offset != KAFKA_STORED_OFFSET) {
                consumer.seek(topicPartition, offset + 1);
            }

            //initial poll for preemptive metadata and offsets negotiation
            return consumer;
        } catch (InvalidOffsetException e) {
            log.error("Invalid offset provided: {} resetting to Earliest Offset", offset, e);
            delegate.processError(new PlatformErrorImpl(e.getMessage(), e.getCause()));
            // setting earliest offset
            this.delegate.initialOffset = EARLIEST_OFFSET;
            return getKafkaConsumer();
        }
    }

    /**
     * Create the Consumer properties and load the properties defined at configuration file
     *
     * @return the Consumer properties
     */
    private Map<String, Object> loadConsumerProps() {
        final Map<String, Object> properties = PropertiesReader.getInstance().loadConsumerProps();

        //fill the deserializing-specific properties and client id
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, topic.toLowerCase());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, topic.toLowerCase() + "Consumer");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass().getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass().getName());
        return properties;
    }

    /**
     * Start the Kafka receiver container
     */
    void start() {
        // creates the kafka consumer
        createConsumer();
        shuttingDown.set(false);
        this.consumerThread.start();
    }

    /**
     * Stop the Kafka receiver container
     */
    void stop() {
        shuttingDown.set(true);
    }

}
