package pcosta.kafka.api;

import java.util.Collection;

/**
 * @author Pedro Costa
 * <p>
 * Contains all the defined message listeners configurations
 */
public interface MessageReceiverConfiguration {

    /**
     * Returns the error listener that should receive and handle platform related errors.
     *
     * @return the error listener
     */
    PlatformErrorListener getErrorListener();

    /**
     * Returns all of the configured listeners.
     * This configuration is final, and cannot be changed.
     *
     * @return the listeners configuration
     */
    Collection<MessageListenerConfiguration<?>> getListeners();

}
