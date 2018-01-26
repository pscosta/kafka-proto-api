package pcosta.kafka.spring;

import pcosta.kafka.api.*;
import pcosta.kafka.configuration.ReceiverConfigurationBuilder;
import pcosta.kafka.spring.annotation.EnableKafkaApiBootstrap;
import pcosta.kafka.spring.annotation.ErrorListener;
import pcosta.kafka.spring.annotation.MessagingListener;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Parser;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextRefreshedEvent;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the {@link KafkaApiBootstrap} class.
 *
 * @author Pedro Costa
 */
public class KafkaApiBootstrapTest {

    private static final String TOPIC1 = "Topic1";
    private static final String TOPIC2 = "Topic2";

    // the bootstrap
    private KafkaApiBootstrap bootstrap;

    @Before
    public void setup() {
        bootstrap = spy(new KafkaApiBootstrap() {
            @Override
            ReceiverConfigurationBuilder createReceiverConfigurationBuilder() {
                return super.createReceiverConfigurationBuilder();
            }
        });
    }

    @Test
    public void test_bootstrap_withoutEnableMessagingBootstrap() {
        final ApplicationContext context = mock(ApplicationContext.class);
        bootstrap.onApplicationEvent(new ContextRefreshedEvent(context));
        assertFalse(bootstrap.initialized);
    }

    @Test
    public void test_bootstrap_startStop_withoutFilters() throws MessagingException {
        executeTest(false);
    }

    @Test
    public void test_bootstrap_startStop_withFilters() throws MessagingException {
        executeTest(true);
    }

    private void executeTest(final boolean useFilters) throws MessagingException {
        final MessagingContext msgContext = mock(MessagingContext.class);
        final ApplicationContext appContext = mock(ApplicationContext.class);

        // configure the application context for the test
        setupApplicationContext(appContext, msgContext, true, useFilters);

        // do the bootstrapping capturing the configuration
        bootstrap.onApplicationEvent(new ContextRefreshedEvent(appContext));
        validateStart(msgContext, appContext, 1, useFilters);

        // try re-initializing, should not happen
        bootstrap.onApplicationEvent(new ContextRefreshedEvent(appContext));
        verify(msgContext, times(1)).createReceiver(any(MessageReceiverConfiguration.class));

        // shutdown the API
        bootstrap.stopListeners(appContext);
        verify(msgContext, times(1)).shutdown();
        assertFalse(bootstrap.initialized);

        // try to start again
        bootstrap.setupListeners(appContext);
        validateStart(msgContext, appContext, 2, useFilters);
    }

    private void validateStart(final MessagingContext msgContext, final ApplicationContext appContext, final int times, final boolean useFilters) throws MessagingException {
        final ArgumentCaptor<MessageReceiverConfiguration> captor = ArgumentCaptor.forClass(MessageReceiverConfiguration.class);
        assertTrue(bootstrap.initialized);
        verify(msgContext, times(times)).createReceiver(captor.capture());

        final MessageReceiverConfiguration configuration = captor.getValue();
        assertNotNull(configuration);
        assertEquals(appContext.getBeansWithAnnotation(ErrorListener.class).values().iterator().next(), configuration.getErrorListener());
        assertEquals(2, configuration.getListeners().size());

        final boolean[] validationRun = new boolean[2];
        for (final MessageListenerConfiguration<?> listenerConfiguration : configuration.getListeners()) {
            int listenerIdx = -1;

            // topic
            assertEquals(1, listenerConfiguration.getTopics().size());
            final String topic = listenerConfiguration.getTopics().iterator().next();

            // assert
            switch (topic) {
                case TOPIC1:
                    assertFalse(validationRun[0]);
                    listenerIdx = 0;
                    break;
                case TOPIC2:
                    assertFalse(validationRun[1]);
                    listenerIdx = 1;
                    break;
                default:
                    fail("unexpected metadata received");
                    break;
            }

            // listeners
            assertEquals(1, listenerConfiguration.getMessageListeners().size());
            if (listenerIdx == 0) {
                assertEquals(SomeMessageListener.class, listenerConfiguration.getMessageListeners().iterator().next().getClass());
            } else {
                assertEquals(SomeOtherMessageListener.class, listenerConfiguration.getMessageListeners().iterator().next().getClass());
            }

            // filters
            assertEquals(!(useFilters && listenerIdx == 0), listenerConfiguration.getMessageFilters().isEmpty());
            if (useFilters && listenerIdx == 0) {
                final MessageFilter filter = listenerConfiguration.getMessageFilters().iterator().next();
                assertThat(filter, instanceOf(SomeMessageFilter.class));
            }

            // message type
            assertNotNull(listenerConfiguration.getMessageType());
            if (listenerIdx == 0) {
                assertEquals(SomeMessage.class, listenerConfiguration.getMessageType());
            } else {
                assertEquals(SomeOtherMessage.class, listenerConfiguration.getMessageType());
            }

            validationRun[listenerIdx] = true;
        }
    }

    @Test
    public void test_bootstrap_withoutBootstrapOfListeners() throws MessagingException {
        final MessagingContext msgContext = mock(MessagingContext.class);
        final ApplicationContext appContext = mock(ApplicationContext.class);

        // configure the application context for the test
        setupApplicationContext(appContext, msgContext, false, false);

        // trigger the bootstrap
        bootstrap.onApplicationEvent(new ContextRefreshedEvent(appContext));

        // validate
        assertFalse(bootstrap.initialized);
        verify(msgContext, never()).createReceiver(any(MessageReceiverConfiguration.class));
    }

    /**
     * Configures the application context for the bootstrapping test
     *
     * @param appContext             the application context
     * @param msgContext             the messaging context
     * @param autoBootstrapListeners to whether or not enable automatic bootstrap of listeners
     */
    private void setupApplicationContext(final ApplicationContext appContext, final MessagingContext msgContext,
                                         final boolean autoBootstrapListeners, final boolean useFilters) {
        // enable the EnableKafkaApiBootstrap annotation
        when(appContext.getBeansWithAnnotation(EnableKafkaApiBootstrap.class)).thenReturn(Collections.singletonMap("configuration", mock(Object.class)));

        final EnableKafkaApiBootstrap enableKafkaApiBootstrap = mock(EnableKafkaApiBootstrap.class);
        when(enableKafkaApiBootstrap.autoRegisterListeners()).thenReturn(autoBootstrapListeners);
        when(appContext.findAnnotationOnBean("configuration", EnableKafkaApiBootstrap.class)).thenReturn(enableKafkaApiBootstrap);

        // put the msg context in the app context
        when(appContext.getBean(KafkaBeanFactory.MESSAGING_CONTEXT)).thenReturn(msgContext);

        // put the error listener in the app context
        when(appContext.getBeansWithAnnotation(ErrorListener.class)).thenReturn(Collections.singletonMap("errorListener", new ErrorListenerOnMethod()));

        // configure the listener
        Map<String, Object> listeners = new LinkedHashMap<>();

        // 1. listener with specific message type configuration
        listeners.put("listener1", new SomeMessageListener());
        configureListener(appContext, "listener1", createConfiguration(
                SomeMessage.class,
                TOPIC1,
                useFilters ? SomeMessageFilter.class : null,
                DefaultExtensionRegistrySupplier.class));

        listeners.put("listener2", new SomeOtherMessageListener());
        configureListener(appContext, "listener2", createConfiguration(
                SomeOtherMessage.class,
                TOPIC2,
                useFilters ? InvalidMessageFilter.class : null,
                DefaultExtensionRegistrySupplier.class));

        when(appContext.getBeansWithAnnotation(MessagingListener.class)).thenReturn(listeners);
    }

    /**
     * Configures a listener bean with the specified configuration
     *
     * @param appContext    the application context
     * @param beanName      the bean name
     * @param configuration the listener configuration
     */
    private void configureListener(final ApplicationContext appContext, final String beanName, final MessagingListener configuration) {
        when(appContext.findAnnotationOnBean(beanName, MessagingListener.class)).thenReturn(configuration);
    }

    /**
     * Creates the configuration for the messaging listener (annotation)
     *
     * @param messageType the message type
     * @param topics      the message topics
     * @param filter      the message filters
     * @return the messaging listener annotation mock
     */
    @SuppressWarnings("unchecked")
    private MessagingListener createConfiguration(final Class<? extends GeneratedMessage> messageType,
                                                  final String topics,
                                                  final Class<? extends MessageFilter> filter,
                                                  final Class<? extends ExtensionRegistrySupplier> extensionRegistry) {
        final MessagingListener configuration = mock(MessagingListener.class);
        when(configuration.message()).thenReturn((Class) messageType);
        when(configuration.topic()).thenReturn(topics);
        when(configuration.extensionRegistry()).thenReturn((Class) extensionRegistry);
        when(configuration.filters()).thenReturn(filter == null ? new Class[0] : new Class[]{filter});
        return configuration;
    }

    // test messages
    @SuppressWarnings("unchecked")
    private static abstract class SomeMessage extends GeneratedMessage {
        @SuppressWarnings("unused")
        public static Parser<SomeMessage> PARSER = mock(Parser.class);
    }

    @SuppressWarnings("unchecked")
    private static abstract class SomeOtherMessage extends GeneratedMessage {
        @SuppressWarnings("unused")
        public static Parser<SomeOtherMessage> PARSER = mock(Parser.class);
    }

    // the message listeners
    private static class SomeMessageListener implements MessageListener<SomeMessage> {
        @Override
        public void onMessage(final MessageMetadata metadata, final SomeMessage message) {
            // empty block
        }
    }

    private static class SomeOtherMessageListener implements MessageListener<SomeOtherMessage> {
        @Override
        public void onMessage(final MessageMetadata metadata, final SomeOtherMessage message) {
            // empty block
        }
    }

    // the message filter
    static class SomeMessageFilter implements MessageFilter {
        @Override
        public boolean filter(final String topic, final Class<?> msgType) {
            return false;
        }

        @Override
        public boolean isEnabled() {
            return true;
        }

        @Override
        public void enable() {
            //noop
        }

        @Override
        public void disable() {
            //noop
        }
    }

    // invalid filter as its a private (non-accessible) class
    private static class InvalidMessageFilter extends SomeMessageFilter {
        //noop
    }

    // test types
    private class ErrorListenerOnMethod implements PlatformErrorListener {
        @ErrorListener
        public void onError(final PlatformError error) {
            // empty block
        }
    }

}
