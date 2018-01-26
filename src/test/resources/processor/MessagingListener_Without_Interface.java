package processor;

import pcosta.kafka.spring.annotation.MessagingListener;
import pcosta.kafka.spring.processor.MessagingListenerProcessorTest;

/**
 * @author Pedro Costa
 */
@MessagingListener(topic = "Topic", message = MessagingListenerProcessorTest.TestMessage.class)
public class MessagingListener_Without_Interface {
}