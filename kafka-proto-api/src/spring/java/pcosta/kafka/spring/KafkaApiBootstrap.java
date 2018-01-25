package pcosta.kafka.spring;

import pcosta.kafka.api.*;
import pcosta.kafka.configuration.ReceiverConfigurationBuilder;
import pcosta.kafka.spring.annotation.*;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.GenericTypeResolver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * The messaging bootstrap that shall scan for the messaging configuration directives and apply then in the messaging
 * context of this application.
 * <p>
 * If any unexpected exception occurs, {@link KafkaApiBootstrapException} are thrown an the initialization is aborted.
 *
 * @author Pedro Costa
 */
class KafkaApiBootstrap implements ApplicationListener<ContextRefreshedEvent> {

    private static final Logger log = LoggerFactory.getLogger(KafkaApiBootstrap.class);

    // the initialized flag
    volatile boolean initialized = false;

    @Override
    public void onApplicationEvent(final ContextRefreshedEvent event) {
        final EnableKafkaApiBootstrap annotation = getMessagingAnnotation(event.getApplicationContext());

        //Init if not initialized already and autoRegisterListeners prop is active
        if (annotation != null && annotation.autoRegisterListeners() && !initialized) {
            start(event.getApplicationContext());
        }
    }

    /**
     * Starts the Messaging application
     *
     * @param context the application context
     */
    synchronized void start(ApplicationContext context) {
        try {
            if (!initialized) {
                final EnableListenerConfiguration configAnnotation = getMessagingConfigurationAnnotation(context);
                final EnableKafkaApiBootstrap annotation = getMessagingAnnotation(context);

                if (configAnnotation == null && annotation != null) {
                    setupListeners(context);
                } else if (configAnnotation != null && annotation != null) {
                    setupListenerWithConfiguration(context);
                }
            }
        } catch (final MessagingException e) {
            throw new IllegalStateException("Unable to bootstrap messaging listeners", e);
        }
    }

    /**
     * Finds the messaging listener configuration annotation at the application context
     *
     * @param applicationContext the application context
     * @return the annotation definition or {@code null} if not found
     */
    private EnableListenerConfiguration getMessagingConfigurationAnnotation(final ApplicationContext applicationContext) {
        final Map<String, Object> beans = applicationContext.getBeansWithAnnotation(EnableListenerConfiguration.class);
        if (beans.size() == 1) {
            final String key = beans.entrySet().iterator().next().getKey();
            return applicationContext.findAnnotationOnBean(key, EnableListenerConfiguration.class);
        }
        return null;
    }

    /**
     * Finds the messaging annotation at the application context
     *
     * @param applicationContext the application context
     * @return the annotation definition or {@code null} if not found
     */
    private EnableKafkaApiBootstrap getMessagingAnnotation(final ApplicationContext applicationContext) {
        final Map<String, Object> beans = applicationContext.getBeansWithAnnotation(EnableKafkaApiBootstrap.class);
        if (beans.size() == 1) {
            final String key = beans.entrySet().iterator().next().getKey();
            return applicationContext.findAnnotationOnBean(key, EnableKafkaApiBootstrap.class);
        }
        return null;
    }

    /**
     * Setups the configured messaging listeners
     *
     * @param context the application context
     * @throws MessagingException if any error occurs while creating the messaging receiver
     */
    synchronized void setupListeners(final ApplicationContext context) throws MessagingException {
        // get the messaging context
        final MessagingContext messagingContext = (MessagingContext) context.getBean(KafkaBeanFactory.MESSAGING_CONTEXT);
        final ReceiverConfigurationBuilder builder = createReceiverConfigurationBuilder();

        createErrorListener(context, builder);
        createModuleAndPlatformListeners(context, builder);
        // finally register all of the listeners
        messagingContext.createReceiver(builder.build());
        // mark as initialized
        this.initialized = true;
    }

    /**
     * Setups the configured messaging listeners
     *
     * @param context the application context
     * @throws MessagingException if any error occurs while creating the messaging receiver
     */
    private synchronized void setupListenerWithConfiguration(final ApplicationContext context) throws MessagingException {
        // get the messaging context
        final MessagingContext messagingContext = (MessagingContext) context.getBean(KafkaBeanFactory.MESSAGING_CONTEXT);

        // register the message listeners
        messagingContext.createReceiver(getReceiverConfiguration(context));

        // mark as initialized
        this.initialized = true;
    }

    /**
     * Stops the configured messaging listeners
     *
     * @param applicationContext the application context
     * @throws MessagingException if any error occurs while stopping the messaging receiver
     */
    synchronized void stopListeners(final ApplicationContext applicationContext) throws MessagingException {
        // get the messaging context
        final MessagingContext messagingContext = (MessagingContext) applicationContext.getBean(KafkaBeanFactory.MESSAGING_CONTEXT);

        // shutdown the context
        messagingContext.shutdown();

        // reset the initialized flag
        this.initialized = false;
    }

    /**
     * Creates the receiver configuration builder to be used for the listeners registering
     *
     * @return the configuration builder
     */
    ReceiverConfigurationBuilder createReceiverConfigurationBuilder() {
        return ReceiverConfigurationBuilder.newBuilder();
    }

    /**
     * Creates the configured platform error listener for the application being deployed
     *
     * @param context the application context
     * @param builder the receiver configuration builder
     */
    @SuppressWarnings("unchecked")
    private void createErrorListener(final ApplicationContext context, final ReceiverConfigurationBuilder builder) throws KafkaApiBootstrapException {
        // get the beans annotated with the listener annotation
        final Map<String, Object> beans = context.getBeansWithAnnotation(ErrorListener.class);
        log.debug("found {} beans annotated with {}", beans.size(), ErrorListener.class);

        // only 1 listener is expected
        if (beans.isEmpty()) {
            throw new KafkaApiBootstrapException("Unable to find an Error listener");
        } else if (beans.size() > 1) {
            throw new KafkaApiBootstrapException("At the most only one bean must be defined with ErrorListener annotation");
        }

        final Map.Entry<String, Object> beanEntry = beans.entrySet().iterator().next();
        log.debug("detected {} as error listener", beanEntry.getKey());
        builder.withErrorListener((PlatformErrorListener) beanEntry.getValue());
    }

    /**
     * Retrieves the {@link MessageReceiverConfiguration} from the respective EnableListenerConfiguration annotated bean
     *
     * @param context the application context
     */
    private MessageReceiverConfiguration getReceiverConfiguration(final ApplicationContext context) throws KafkaApiBootstrapException {
        // get the beans annotated with the listener configuration annotation
        final Map<String, Object> beans = context.getBeansWithAnnotation(EnableListenerConfiguration.class);
        log.debug("found {} beans annotated with {}", beans.size(), EnableListenerConfiguration.class);

        // only 1 listener is expected
        if (beans.isEmpty()) {
            throw new KafkaApiBootstrapException("Unable to find an EnableListenerConfiguration bean");
        } else if (beans.size() > 1) {
            throw new KafkaApiBootstrapException("At the most only one bean must be defined with EnableListenerConfiguration annotation");
        }

        final Map.Entry<String, Object> beanEntry = beans.entrySet().iterator().next();
        log.debug("detected {} as EnableListenerConfiguration bean", beanEntry.getKey());
        return ((ReceiverConfigurationProvider) beanEntry.getValue()).getReceiverConfiguration();
    }

    /**
     * Creates the configured inbound listeners for the application being deployed
     *
     * @param context the application context
     * @param builder the receiver configuration builder
     */
    @SuppressWarnings("unchecked")
    private void createModuleAndPlatformListeners(final ApplicationContext context, final ReceiverConfigurationBuilder builder) throws KafkaApiBootstrapException {
        // get the beans annotated with the listener annotation
        final Map<String, Object> beans = context.getBeansWithAnnotation(MessagingListener.class);
        log.debug("found {} beans annotated with {}", beans.size(), MessagingListener.class);

        // create the configuration for each bean
        for (final Map.Entry<String, Object> beanEntry : beans.entrySet()) {
            // get the configuration
            final MessagingListener configuration = context.findAnnotationOnBean(beanEntry.getKey(), MessagingListener.class);
            log.debug("configuration for {} = {}", beanEntry.getKey(), configuration);

            // get the generic type in order to fit the specific listener configuration if none is specified
            final Class<? extends Message> type;
            if (configuration.message() == DEFAULT_MESSAGE_TYPE.class) {
                type = (Class<? extends Message>) GenericTypeResolver.resolveTypeArgument(beanEntry.getValue().getClass(), MessageListener.class);

                if (type == null) {
                    throw new KafkaApiBootstrapException("Invalid listener definition: unable to determine the message " + "type for bean " + beanEntry.getKey());
                }
            } else {
                type = configuration.message();
            }
            log.debug("generic type of {} message listener is {}", beanEntry.getKey(), type);

            // create the listener
            builder.newListener()
                    .addHandler(((MessageListener<Message>) beanEntry.getValue()))
                    .addTopics(configuration.topic())
                    .addInitialOffset(((MessageListener<Message>) beanEntry.getValue()).initialOffset(configuration.topic()))
                    .addTopicPartition(configuration.partition())
                    .withMessageType((Class<Message>) configuration.message())
                    .withExtensionRegistry(createExtensionRegistry(configuration.extensionRegistry()))
                    .addFilters(createMessageFilters(configuration.filters(), context))
                    .buildListener();
        }
    }

    /**
     * Creates the Extension Registry supplied from the given {@link ExtensionRegistrySupplier}
     *
     * @param supplier the Extension Registry supplier
     * @return the produced {@link ExtensionRegistry}
     */
    private ExtensionRegistry createExtensionRegistry(final Class<? extends ExtensionRegistrySupplier> supplier) {
        // create an instance of the Extension Registry Supplier
        final ExtensionRegistrySupplier instance;
        try {
            instance = supplier.newInstance();
        } catch (final InstantiationException | IllegalAccessException e) {
            throw new MessagingException("Unable to create an instance of " + supplier, e);
        }

        // create the ExtensionRegistry
        return instance.get();
    }

    /**
     * Creates the message filters for the given configuration
     *
     * @param filterClasses      the message filter classes
     * @param applicationContext the current application context
     * @return the message filters
     */
    private Collection<MessageFilter> createMessageFilters(final Class<? extends MessageFilter>[] filterClasses, final ApplicationContext applicationContext) {
        final Collection<MessageFilter> filters = new ArrayList<>(filterClasses.length);
        for (final Class<? extends MessageFilter> clazz : filterClasses) {
            MessageFilter instance = applicationContext.getBean(clazz);
            if (instance == null) {
                log.debug("unable to find bean for message filter " + clazz.getSimpleName());
                try {
                    instance = clazz.newInstance();
                } catch (InstantiationException | IllegalAccessException e) {
                    log.error("unable to find instantiate message filter " + clazz.getSimpleName() + " as fallback", e);
                }
            }
            if (instance != null) {
                filters.add(instance);
            }
        }
        return filters;
    }

}
