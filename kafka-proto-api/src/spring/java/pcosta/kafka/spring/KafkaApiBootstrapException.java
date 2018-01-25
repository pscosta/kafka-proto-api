package pcosta.kafka.spring;

import pcosta.kafka.api.MessagingException;

/**
 * This exception denotes an error while initializing/bootstrapping the Kafka API configuration.
 *
 * @author Pedro Costa
 */
public class KafkaApiBootstrapException extends MessagingException {

    /**
     * Constructs the exception with the specified error message
     *
     * @param message the error message
     */
    public KafkaApiBootstrapException(final String message) {
        super(message);
    }
}
