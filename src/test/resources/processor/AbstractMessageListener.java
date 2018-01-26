package processor;

import pcosta.kafka.api.MessageListener;
import com.google.protobuf.GeneratedMessage;

/**
 * @author Pedro Costa
 */
public abstract class AbstractMessageListener<M extends GeneratedMessage> implements MessageListener<M> {

}
