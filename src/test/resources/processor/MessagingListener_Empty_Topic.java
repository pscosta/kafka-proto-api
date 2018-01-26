package processor;

import pcosta.kafka.api.MessageListener;
import pcosta.kafka.api.MessageMetadata;
import pcosta.kafka.spring.annotation.MessagingListener;
import pcosta.kafka.spring.processor.MessagingListenerProcessorTest;

/**
 * @author Pedro Costa
 */
@MessagingListener(topic = "", message = MessagingListenerProcessorTest.TestMessage.class)
public class MessagingListener_Empty_Topic implements MessageListener<MessagingListenerProcessorTest.TestMessage> {

    @Override
    public void onMessage(final MessageMetadata messageMetadata, final MessagingListenerProcessorTest.TestMessage message) {
        // empty
    }
}
