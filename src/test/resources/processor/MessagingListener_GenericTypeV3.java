package processor;

import pcosta.kafka.api.MessageListener;
import pcosta.kafka.api.MessageMetadata;
import pcosta.kafka.spring.annotation.MessagingListener;
import com.google.protobuf.GeneratedMessageV3;

/**
 * @author Pedro Costa
 */
@MessagingListener(topic = "Topic", message = GeneratedMessageV3.class)
public class MessagingListener_GenericTypeV3 implements MessageListener<GeneratedMessageV3> {

    @Override
    public void onMessage(final MessageMetadata messageMetadata, final GeneratedMessageV3 message) {
        // empty
    }
}
