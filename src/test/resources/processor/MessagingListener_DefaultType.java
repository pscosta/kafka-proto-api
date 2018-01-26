package processor;

import pcosta.kafka.api.MessageListener;
import pcosta.kafka.api.MessageMetadata;
import pcosta.kafka.spring.annotation.DEFAULT_MESSAGE_TYPE;
import pcosta.kafka.spring.annotation.MessagingListener;

/**
 * @author Pedro Costa
 */
@MessagingListener(topic = "Topic", message = DEFAULT_MESSAGE_TYPE.class)
public class MessagingListener_DefaultType implements MessageListener<DEFAULT_MESSAGE_TYPE> {

    @Override
    public void onMessage(final MessageMetadata messageMetadata, final DEFAULT_MESSAGE_TYPE message) {
        // empty
    }
}
