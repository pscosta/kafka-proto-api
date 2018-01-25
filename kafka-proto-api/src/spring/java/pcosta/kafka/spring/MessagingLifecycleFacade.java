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
    private final KafkaApiBootstrap kafkaApiBootstrap;

    /**
     * Default lifecycle constructor
     *
     * @param kafkaApiBootstrap the messaging bootstrap
     */
    MessagingLifecycleFacade(final KafkaApiBootstrap kafkaApiBootstrap) {
        this.kafkaApiBootstrap = kafkaApiBootstrap;
    }

    /**
     * Setup and starts the messaging configured messaging listeners at the specified application context
     *
     * @param applicationContext the application context
     * @throws MessagingException if any error occurs while creating the messaging receiver
     */
    public void start(final ApplicationContext applicationContext) throws MessagingException {
        kafkaApiBootstrap.start(applicationContext);
    }

    /**
     * Stops the previously configured and bootstrapped messaging listeners
     *
     * @param applicationContext the application context
     * @throws MessagingException if any error occurs while shutting down the messaging receiver
     */
    public void stop(final ApplicationContext applicationContext) throws MessagingException {
        kafkaApiBootstrap.stopListeners(applicationContext);
    }

}
