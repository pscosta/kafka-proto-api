package pcosta.kafka.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pcosta.kafka.api.MessageKey;

import java.util.Objects;

/**
 * @author Pedro Costa
 * <p>
 * The Implementation of a String Kafka message key
 * The Key must respect the following structure:
 * <br>
 * {@code <topic name>|<fully qualified message name>}
 */
public class StringMessageKey<KEY> implements MessageKey<KEY> {

    private static final Logger log = LoggerFactory.getLogger(StringMessageKey.class);

    //delimiter for kafka keys: <topic name>|<fully qualified message name>
    private static final String KEY_DELIMITER = "|";

    //the original message key String
    private String messageKey;
    //the sender topic
    private String srcTopic;
    //the proto message type
    private String messageType;

    /**
     * Constructor for outgoing messages
     *
     * @param srcTopic    the sender topic
     * @param messageType the proto message type
     */
    StringMessageKey(String srcTopic, String messageType) {
        this.messageType = messageType;
        this.srcTopic = srcTopic;
        this.messageKey = generateKey();
    }

    /**
     * Constructor for incoming messages
     *
     * @param key the incoming message key to be deserialized
     * @throws IllegalArgumentException for errors during key deserializing process
     */
    StringMessageKey(KEY key) throws IllegalArgumentException {
        Objects.requireNonNull(key, "Received null message key");
        deserializeKey(key);
    }

    @Override
    public String generateKey() {
        return srcTopic + KEY_DELIMITER + messageType;
    }

    @Override
    public void deserializeKey(KEY key) throws IllegalArgumentException {
        final String[] keyElements = key.toString().split("\\|");
        this.messageKey = key.toString();

        if (keyElements.length != 2) {
            log.debug("unknown message key format: {}", key);
            return;
        }

        this.srcTopic = keyElements[0];
        this.messageType = keyElements[1];
    }

    public String getSrcTopic() {
        return srcTopic;
    }

    public String getMessageType() {
        return messageType;
    }

    @Override
    public String getKey() {
        return messageKey;
    }
}
