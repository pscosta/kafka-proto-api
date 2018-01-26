package pcosta.kafka.configuration;

import pcosta.kafka.api.MessageFilter;
import pcosta.kafka.api.MessageListener;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.GeneratedMessage;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static pcosta.kafka.api.MessageListener.LATEST_OFFSET;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for the {@link MessageListenerConfigurationImpl}.
 *
 * @author Pedro Costa
 */
public class MessageListenerConfigurationImplTest {

    @Test
    @SuppressWarnings("unchecked")
    public void test_gettersCorrection() {
        // Prepare: mock
        final Collection<MessageListener> listeners = Collections.singletonList(mock(MessageListener.class));
        final Collection<String> topics = Arrays.asList("GoodTopic", "GoodTopic2");
        final Class messageType = GeneratedMessage.class;
        final Collection<MessageFilter> filters = Collections.singletonList(mock(MessageFilter.class));
        final ExtensionRegistry emptyRegistry = ExtensionRegistry.getEmptyRegistry();

        // Call: initialize the configuration
        final MessageListenerConfigurationImpl configuration = new MessageListenerConfigurationImpl(
                listeners, filters, topics, emptyRegistry, messageType, 0, LATEST_OFFSET);

        final Collection configuredListeners = configuration.getMessageListeners();
        final Collection configuredTopics = configuration.getTopics();
        final Class configuredType = configuration.getMessageType();
        final Collection configuredFilters = configuration.getMessageFilters();
        final long offset = configuration.getOffset();
        final int partition = configuration.getPartition();

        // Assert
        assertEquals(listeners, configuredListeners);
        assertEquals(topics, configuredTopics);
        assertEquals(messageType, configuredType);
        assertEquals(filters, configuredFilters);
        assertEquals(offset, LATEST_OFFSET);
        assertEquals(partition, 0);
    }

}
