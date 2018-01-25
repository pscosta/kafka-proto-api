package pcosta.kafka.spring;

import pcosta.kafka.api.MessageListener;
import pcosta.kafka.api.MessageReceiverConfiguration;

/**
 * @author Pedro Costa
 * <p>
 * Provides the {@link MessageReceiverConfiguration} with message listener configurations
 */
public interface ReceiverConfigurationProvider {

    /**
     * Provides a {@link MessageReceiverConfiguration} with all the {@link MessageListener}s to be registered
     *
     * @return the MessageReceiverConfiguration instance
     */
    MessageReceiverConfiguration getReceiverConfiguration();

}
