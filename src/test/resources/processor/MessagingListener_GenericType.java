package processor;

import pcosta.kafka.api.MessageListener;
import pcosta.kafka.api.MessageMetadata;
import pcosta.kafka.api.annotation.MessagingListener;
import com.google.protobuf.GeneratedMessage;

/**
 * @author Pedro Costa
 */
@MessagingListener(topic = "Topic", message = GeneratedMessage.class)
public class MessagingListener_GenericType implements MessageListener<GeneratedMessage> {

    @Override
    public void onMessage(final MessageMetadata messageMetadata, final GeneratedMessage message) {
        // empty
    }
}
