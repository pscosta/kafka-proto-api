package processor;

import pcosta.kafka.api.MessageMetadata;
import pcosta.kafka.spring.annotation.MessagingListener;
import pcosta.kafka.spring.processor.MessagingListenerProcessorTest;

/**
 * @author Pedro Costa
 */
@MessagingListener(topic = "Topic", message = MessagingListenerProcessorTest.TestMessage.class)
public class MessagingListener_Extends_AbstractMessageListener extends AbstractMessageListener<MessagingListenerProcessorTest.TestMessage> {

    @Override
    public void onMessage(final MessageMetadata messageMetadata, final MessagingListenerProcessorTest.TestMessage message) {
        // empty
    }
}