package pcosta.kafka.internal;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.Map;

/**
 * @author Pedro Costa
 * <p>
 * Implementation of a Kafka protobuf messages sender
 */
class KafkaSender<KEY, OUT> {

    private static final Logger log = LoggerFactory.getLogger(KafkaSender.class);

    private final String dstTopic;
    private final Serializer<KEY> keySerializer;
    private final Serializer<OUT> valueSerializer;
    private final Map<String, Object> senderProperties;

    // the kafka receiver template
    private final KafkaTemplate<KEY, OUT> template;

    // the kafka message producer factory
    private DefaultKafkaProducerFactory<KEY, OUT> producerFactory;

    /**
     * The default constructor
     *
     * @param dstTopic        this sender's destination Topic
     * @param keySerializer   the message key serializer
     * @param valueSerializer the protobuf message valueSerializer
     */
    KafkaSender(String dstTopic, Serializer<KEY> keySerializer, Serializer<OUT> valueSerializer) {
        this.dstTopic = dstTopic;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;

        // load the properties defined at kafka properties file
        this.senderProperties = senderProps();
        // create the container instance
        this.template = createTemplate();
    }

    /**
     * @param dstTopic        this sender's dstTopic
     * @param keySerializer   the  message key Serializer
     * @param valueSerializer the protobuf message value Serializer
     * @param properties      the sender properties
     */
    KafkaSender(String dstTopic, Serializer<KEY> keySerializer, Serializer<OUT> valueSerializer, Map<String, Object> properties) {
        this.dstTopic = dstTopic;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;

        // load the properties defined at kafka properties file
        this.senderProperties = properties;
        // create the container instance
        this.template = createTemplate();
    }

    /**
     * Send a protobuf message to this dstTopic
     *
     * @param key     the message kafka key
     * @param message the message to be sent to this dstTopic
     * @return the result {@link ListenableFuture}
     */
    ListenableFuture<SendResult<KEY, OUT>> send(KEY key, OUT message) {
        try {
            // send the message
            return template.sendDefault(key, message);
        } catch (Exception e) {
            // wrap all errors
            final SettableListenableFuture<SendResult<KEY, OUT>> result = new SettableListenableFuture<>();
            result.setException(e);
            return result;
        }
    }

    /**
     * Creates a Kafka Template
     *
     * @return the created Kafka Template
     */
    private KafkaTemplate<KEY, OUT> createTemplate() {
        log.info("Creating Kafka Sender for Topic: {}", dstTopic);
        this.producerFactory = new DefaultKafkaProducerFactory<>(senderProperties, keySerializer, valueSerializer);

        // explicit kafka producer creation
        producerFactory.createProducer();

        final KafkaTemplate<KEY, OUT> kafkaTemplate = new KafkaTemplate<>(producerFactory);
        kafkaTemplate.setDefaultTopic(dstTopic);

        log.info("Kafka Sender for Topic: {} was successfully created", dstTopic);
        return kafkaTemplate;
    }

    /**
     * Create the Sender properties
     *
     * @return the Sender properties
     */
    private Map<String, Object> senderProps() {
        final Map<String, Object> properties = PropertiesReader.getInstance().loadSenderProps();

        //fill the deserializing-specific properties
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka_proto_api" + "->" + dstTopic.toLowerCase());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getClass().getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getClass().getName());
        return properties;
    }

    /**
     * Stops the Kafka sender
     */
    void stop() {
        this.producerFactory.stop();
    }
}
