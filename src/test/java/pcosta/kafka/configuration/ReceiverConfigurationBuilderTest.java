package pcosta.kafka.configuration;

import com.google.protobuf.Message;
import org.junit.Test;
import pcosta.kafka.api.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for the {@link ReceiverConfigurationBuilder}.
 *
 * @author Pedro Costa
 */
public class ReceiverConfigurationBuilderTest {

    @Test(expected = NullPointerException.class) // no error listener
    public void test_emptyBuild() {
        ReceiverConfigurationBuilder.newBuilder().build();
    }

    @Test(expected = NullPointerException.class)
    public void test_invalidErrorListener() {
        ReceiverConfigurationBuilder.newBuilder().withErrorListener(null);
    }

    @Test(expected = IllegalStateException.class)
    public void test_defineErrorListenerTwice() {
        ReceiverConfigurationBuilder.newBuilder()
                .withErrorListener(mock(PlatformErrorListener.class))
                .withErrorListener(mock(PlatformErrorListener.class));
    }

    @Test
    public void test_defineErrorListenerTwiceButSame() {
        final PlatformErrorListener listener = mock(PlatformErrorListener.class);
        ReceiverConfigurationBuilder.newBuilder()
                .withErrorListener(listener)
                .withErrorListener(listener);
    }

    @Test(expected = IllegalArgumentException.class)
    @SuppressWarnings("unchecked")
    public void test_defineTopicsTwice() {
        ReceiverConfigurationBuilder.newBuilder()
                .newListener(mock(MessageListener.class), Message.class, "GoodTopic", "GoodTopic")
                .withErrorListener(mock(PlatformErrorListener.class))
                .build();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test_buildPlatformOnly() {
        final MessageReceiverConfiguration configuration = ReceiverConfigurationBuilder.newBuilder()
                .withErrorListener(mock(PlatformErrorListener.class))
                .newListener(mock(MessageListener.class), Message.class, "GoodTopic")
                .newListener(mock(MessageListener.class), Message.class, "GoodTopic2", "GoodTopic3")
                .build();

        assertNotNull(configuration);
        assertNotNull(configuration.getErrorListener());
        assertFalse(configuration.getListeners().isEmpty());
        assertEquals(2, configuration.getListeners().size());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test_buildPlatformOnlyWithOffset() {
        final MessageReceiverConfiguration configuration = ReceiverConfigurationBuilder.newBuilder()
                .withErrorListener(mock(PlatformErrorListener.class))
                .newListener(mock(MessageListener.class), Message.class, -2L, 0, "GoodTopic")
                .newListener(mock(MessageListener.class), Message.class, -2L, 0, "GoodTopic2", "GoodTopic3")
                .build();

        assertNotNull(configuration);
        assertNotNull(configuration.getErrorListener());
        assertFalse(configuration.getListeners().isEmpty());
        assertEquals(2, configuration.getListeners().size());

        configuration.getListeners().forEach(listener -> {
            assertEquals(-2L, listener.getOffset());
            assertEquals(0, listener.getPartition());
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test_build_noListenersDefined() {
        final MessageReceiverConfiguration configuration = ReceiverConfigurationBuilder.newBuilder()
                .withErrorListener(mock(PlatformErrorListener.class))
                .build();

        assertNotNull(configuration);
        assertNotNull(configuration.getErrorListener());
        assertTrue(configuration.getListeners().isEmpty());
        assertEquals(0, configuration.getListeners().size());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test_newListenerWithConfiguration() {
        // mock
        final MessageListener listener = mock(MessageListener.class);
        final String topic = "GoodTopic";

        // build the configuration
        final MessageReceiverConfiguration configuration = ReceiverConfigurationBuilder.newBuilder()
                .newListener(listener, Message.class, topic)
                .withErrorListener(mock(PlatformErrorListener.class))
                .build();

        // assert builder configuration construction
        assertNotNull(configuration);
        assertNotNull(configuration.getListeners());
        assertEquals(1, configuration.getListeners().size());

        final MessageListenerConfiguration<?> listenerConfiguration = configuration.getListeners().iterator().next();
        assertEquals(listener, listenerConfiguration.getMessageListeners().iterator().next());
        assertEquals(Message.class, listenerConfiguration.getMessageType());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test_oneListeners() {
        // mock
        final MessageListener listener = mock(MessageListener.class);
        final MessageFilter filter = mock(MessageFilter.class);
        final String topic1 = "GoodTopic";
        final String topic2 = "GoodTopic2";

        final PlatformErrorListener errorListener = mock(PlatformErrorListener.class);

        // build the configuration
        final MessageReceiverConfiguration configuration = ReceiverConfigurationBuilder.newBuilder()
                .withErrorListener(errorListener)
                .newListener()
                .addFilter(filter)
                .addTopics(topic1)
                .addHandler(listener)
                .withMessageType(Message.class)
                .addInitialOffset(-2L)
                .addTopicPartition(0)
                .buildListener()
                .newListener()
                .addTopics(topic2)
                .addFilter(filter)
                .addHandler(listener)
                .addInitialOffset(-2L)
                .addTopicPartition(0)
                .withMessageType(Message.class)
                .buildListener()
                .build();

        // assert builder configuration construction
        assertNotNull(configuration);
        assertNotNull(configuration.getErrorListener());
        assertNotNull(configuration.getListeners());
        assertEquals(errorListener, configuration.getErrorListener());
        assertEquals(2, configuration.getListeners().size());

        for (final MessageListenerConfiguration<?> listenerConfiguration : configuration.getListeners()) {
            assertEquals(1, listenerConfiguration.getMessageListeners().size());
            assertEquals(-2L, listenerConfiguration.getOffset());
            assertEquals(0, listenerConfiguration.getPartition());
            assertEquals(listener, listenerConfiguration.getMessageListeners().iterator().next());
            assertEquals(Message.class, listenerConfiguration.getMessageType());
            assertEquals(1, listenerConfiguration.getMessageFilters().size());
        }
    }

}
