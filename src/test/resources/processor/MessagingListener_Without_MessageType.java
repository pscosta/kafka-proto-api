package processor;

import pcosta.kafka.api.MessageListener;
import pcosta.kafka.api.annotation.MessagingListener;
import pcosta.kafka.spring.processor.MessagingListenerProcessorTest;

/**
 * @author Pedro Costa
 */
@MessagingListener(topic = "Topic", message = MessagingListenerProcessorTest.TestMessage.class)
public class MessagingListener_Without_MessageType implements MessageListener {
}