package pcosta.kafka.api;

/**
 * @author Pedro Costa
 * <p>
 * The interfaces that represents a kafka key that comprises source topic and message type information
 */
public interface MessageKey<KEY> {

    /**
     * Deserializes the received message key and extracts the sender {@code String} and message type
     *
     * @param key the received key to be deserialized
     * @throws IllegalArgumentException if the key is malformed or the received topic is not known
     */
    void deserializeKey(KEY key) throws IllegalArgumentException;

    /**
     * Generates a message key based on the sender topic and the received message Class
     *
     * @return the generated message key
     */
    String generateKey();
}
