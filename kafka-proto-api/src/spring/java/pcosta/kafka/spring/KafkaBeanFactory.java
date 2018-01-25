package pcosta.kafka.spring;

import com.google.protobuf.Message;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import pcosta.kafka.api.MessageProducer;
import pcosta.kafka.api.MessagingContext;
import pcosta.kafka.api.MessagingException;
import pcosta.kafka.api.MessagingFactory;
import pcosta.kafka.internal.MessagingFactoryImpl;
import pcosta.kafka.internal.ProtobufSerializer;
import pcosta.kafka.spring.annotation.EnableMessagingBootstrap;
import pcosta.kafka.spring.annotation.ProtoKafkaSender;
import pcosta.kafka.spring.annotation.StringKafkaSender;

import java.util.Map;

/**
 * @author Pedro Costa
 * <p>
 * Factory for several context-related beans
 */
@Configuration
public class KafkaBeanFactory {

    private static final Logger log = LoggerFactory.getLogger(KafkaBeanFactory.class);

    // bean names definitions
    static final String MESSAGING_CONTEXT = "kafkaMessagingContext";

    @Bean(name = MESSAGING_CONTEXT, destroyMethod = "shutdown")
    @Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
    public MessagingContext messagingContext(final MessagingFactory messagingFactory) throws MessagingException {
        log.info("initializing Kafka API context");
        try {
            final MessagingContext context = messagingFactory.createContext();
            log.info("Kafka API context initialized");
            return context;
        } catch (final MessagingException e) {
            log.error("error initializing Kafka API context");
            throw e;
        }
    }

    @ProtoKafkaSender
    @Bean(name = "kafkaMessageProducer")
    @Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
    public MessageProducer<Message> messageProducer(final MessagingContext context) throws MessagingException {
        return context.createProducer("string-proto", new StringSerializer(), new ProtobufSerializer());
    }

    @StringKafkaSender
    @Bean(name = "kafkaStringMessageProducer")
    @Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
    public MessageProducer<String> stringMessageProducer(final MessagingContext context) throws MessagingException {
        return context.createProducer("string-string", new StringSerializer(), new StringSerializer());
    }

    @Bean(name = "kafkaMessagingFactory")
    @Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
    public MessagingFactory messagingFactory() {
        return new MessagingFactoryImpl();
    }

    @Bean(name = "kafkaMessagingBootstrap")
    @Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
    public MessagingBootstrap messagingBootstrap() {
        return new MessagingBootstrap();
    }

    @Bean(name = "kafkaMessagingLifecycleFacade")
    @Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
    public MessagingLifecycleFacade messagingLifecycleFacade(final MessagingBootstrap messagingBootstrap) {
        return new MessagingLifecycleFacade(messagingBootstrap);
    }

    /**
     * Returns the application {@code Topic} of the currently being deployed context
     *
     * @param beanFactory the bean factory for this context
     * @return the application module {@code Topic}
     */
    private String getTopic(final ListableBeanFactory beanFactory) {
        // get all of the beans annotated with EnableMessagingBootstrap, only 1 must be present!
        final Map<String, Object> beans = beanFactory.getBeansWithAnnotation(EnableMessagingBootstrap.class);

        // validate the beans
        if (beans.size() != 1) {
            throw new IllegalStateException("unable to properly bootstrap Kafka API, expected one and only one "
                    + "class annotated with " + EnableMessagingBootstrap.class.getSimpleName());
        }

        // get the bean
        final Map.Entry<String, Object> bean = beans.entrySet().iterator().next();
        // the extract the application topic
        return beanFactory.findAnnotationOnBean(bean.getKey(), EnableMessagingBootstrap.class).topic();
    }

}
