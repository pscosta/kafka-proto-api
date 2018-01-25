package processor;

import pcosta.kafka.api.MessageListener;
import pcosta.kafka.spring.annotation.MessagingListener;
import pcosta.kafka.spring.processor.MessagingListenerProcessorTest;

/**
 * @author Pedro Costa
 */
@MessagingListener(topic = "Topic", message = MessagingListenerProcessorTest.TestMessage.class)
public interface MessagingListener_At_Interface extends MessageListener<MessagingListenerProcessorTest.TestMessage> {
}
