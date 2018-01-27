package processor;

import pcosta.kafka.api.MessageReceiverConfiguration;
import pcosta.kafka.spring.ReceiverConfigurationProvider;
import pcosta.kafka.api.annotation.EnableListenerConfiguration;

/**
 * @author Pedro Costa
 */
public class EnableConfigListener_Method implements ReceiverConfigurationProvider {

    @EnableListenerConfiguration
    @Override
    public MessageReceiverConfiguration getReceiverConfiguration() {
        // empty
    }

}
