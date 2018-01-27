package pcosta.kafka.api;

/**
 * This factory should act as a messaging bootstrap for a given application, initializing their specific context.
 * Also, implementations must adhere to the {@link MessagingContext} contract
 *
 * @author Pedro Costa
 */
public interface MessagingFactory {

    /**
     * Creates a kafka messaging context
     *
     * @return the kafka context
     * @throws MessagingException if any error occurs while initializing the kafka context
     */
    MessagingContext createContext() throws MessagingException;
}
