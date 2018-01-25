package pcosta.kafka.api;

/**
 * This exception denotes an error while performing an operation by the Kafka API, such as:
 * <ul>
 * <li>Registering a receiver</li>
 * <li>Receiving a message</li>
 * <li>Registering a producer</li>
 * <li>Sending a message</li>
 * </ul>
 *
 * @author Pedro Costa
 */
public class MessagingException extends RuntimeException {

    /**
     * Constructs the exception with the specified error message
     *
     * @param message the error message
     */
    public MessagingException(final String message) {
        super(message);
    }

    /**
     * Constructs the exception with the specified error message and original cause
     *
     * @param message   the error message
     * @param throwable the original error cause
     */
    public MessagingException(final String message, final Throwable throwable) {
        super(message, throwable);
    }
}
