package pcosta.kafka.spring;

import pcosta.kafka.api.MessagingException;
import org.springframework.context.ApplicationContext;

/**
 * The messaging lifecycle facade for the start and stop of the Kafka API
 *
 * @author Pedro Costa
 */
public class MessagingLifecycleFacade {

    // the messaging bootstrap
    private final MessagingBootstrap messagingBootstrap;

    /**
     * Default lifecycle constructor
     *
     * @param messagingBootstrap the messaging bootstrap
     */
    MessagingLifecycleFacade(final MessagingBootstrap messagingBootstrap) {
        this.messagingBootstrap = messagingBootstrap;
    }

    /**
     * Setup and starts the messaging configured messaging listeners at the specified application context
     *
     * @param applicationContext the application context
     * @throws MessagingException if any error occurs while creating the messaging receiver
     */
    public void start(final ApplicationContext applicationContext) throws MessagingException {
        messagingBootstrap.start(applicationContext);
    }

    /**
     * Stops the previously configured and bootstrapped messaging listeners
     *
     * @param applicationContext the application context
     * @throws MessagingException if any error occurs while shutting down the messaging receiver
     */
    public void stop(final ApplicationContext applicationContext) throws MessagingException {
        messagingBootstrap.stopListeners(applicationContext);
    }

}
