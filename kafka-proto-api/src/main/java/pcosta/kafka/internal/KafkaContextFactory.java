package pcosta.kafka.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pcosta.kafka.api.MessagingContext;
import pcosta.kafka.api.MessagingException;
import pcosta.kafka.api.MessagingFactory;

import javax.inject.Singleton;

/**
 * Implementation of the {@link MessagingFactory}.
 */
@Singleton
public class KafkaContextFactory implements MessagingFactory {

    // the logger
    private static final Logger log = LoggerFactory.getLogger(KafkaContextFactory.class);

    //the singleton messaging context
    private KafkaContext kafkaContext;

    private final Object lock = new Object();

    @Override
    public MessagingContext createContext() throws MessagingException {
        log.info("creating the kafka api context");
        synchronized (lock) {
            if (kafkaContext == null) {
                this.kafkaContext = new KafkaContext();
            }
        }
        return kafkaContext;
    }
}