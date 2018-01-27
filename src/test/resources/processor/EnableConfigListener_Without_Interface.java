package processor;

import pcosta.kafka.api.MessageReceiverConfiguration;
import pcosta.kafka.api.annotation.EnableListenerConfiguration;

/**
 * @author Pedro Costa
 */
@EnableListenerConfiguration
public class EnableConfigListener_Without_Interface {

    public MessageReceiverConfiguration getReceiverConfiguration() {
        return null;
    }

}
