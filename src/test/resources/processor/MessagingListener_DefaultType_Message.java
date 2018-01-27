package processor;

import pcosta.kafka.api.MessageListener;
import pcosta.kafka.api.MessageMetadata;
import pcosta.kafka.api.annotation.MessagingListener;
import com.google.protobuf.Message;

/**
 * @author Pedro Costa
 */
@MessagingListener(topic = "Topic")
public class MessagingListener_DefaultType_Message implements MessageListener<Message> {

    @Override
    public void onMessage(final MessageMetadata messageMetadata, final Message message) {
        // empty
    }
}
