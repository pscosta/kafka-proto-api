package processor;

import pcosta.kafka.api.MessageListener;
import pcosta.kafka.api.MessageMetadata;
import pcosta.kafka.api.annotation.MessagingListener;
import pcosta.kafka.message.KafkaMessageProto.KafkaMessage;

/**
 * @author Pedro Costa
 */
@MessagingListener(topic = "Topic", message = KafkaMessage.class)
public class MessagingListener_KafkaMessage implements MessageListener<KafkaMessage> {

    @Override
    public void onMessage(final MessageMetadata messageMetadata, final KafkaMessage message) {
        // empty
    }
}
