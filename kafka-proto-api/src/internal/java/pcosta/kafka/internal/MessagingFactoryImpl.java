package pcosta.kafka.internal;

import pcosta.kafka.api.MessagingContext;
import pcosta.kafka.api.MessagingException;
import pcosta.kafka.api.MessagingFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;

/**
 * Implementation of the {@link MessagingFactory}.
 */
@Singleton
public class MessagingFactoryImpl implements MessagingFactory {

    // the logger
    private static final Logger log = LoggerFactory.getLogger(MessagingFactoryImpl.class);

    //the singleton messaging context
    private KafkaContext kafkaContext;

    private final Object lock = new Object();

    @Override
    public MessagingContext createContext(String topic) throws MessagingException {
        log.info("creating the kafka api context fot topic: {}", topic);
        synchronized (lock) {
            if (kafkaContext == null) {
                this.kafkaContext = new KafkaContext(topic);
            }
        }
        return kafkaContext;
    }

}
