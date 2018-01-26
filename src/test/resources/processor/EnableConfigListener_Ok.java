package processor;

import pcosta.kafka.api.MessageReceiverConfiguration;
import pcosta.kafka.spring.ReceiverConfigurationProvider;
import pcosta.kafka.spring.annotation.EnableListenerConfiguration;

/**
 * @author Pedro Costa
 */
@EnableListenerConfiguration
public class EnableConfigListener_Ok implements ReceiverConfigurationProvider {

    @Override
    public MessageReceiverConfiguration getReceiverConfiguration() {
        return null;
    }

}
