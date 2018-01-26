package pcosta.kafka.configuration;

import pcosta.kafka.api.MessageListenerConfiguration;
import pcosta.kafka.api.PlatformErrorListener;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for the {@link MessageReceiverConfigurationImpl}.
 *
 * @author Pedro Costa
 */
public class MessageReceiverConfigurationImplTest {

    @Test
    public void test_gettersCorrection() {
        // Prepare
        final PlatformErrorListener errorListener = mock(PlatformErrorListener.class);
        final MessageListenerConfiguration<?> listenerConfiguration = mock(MessageListenerConfiguration.class);
        final Collection<MessageListenerConfiguration<?>> listeners = Collections.singletonList(listenerConfiguration);

        // Call: initialize the configuration
        final MessageReceiverConfigurationImpl configuration = new MessageReceiverConfigurationImpl(errorListener, listeners);

        // Assert
        assertEquals(errorListener, configuration.getErrorListener());
        assertEquals(listeners, configuration.getListeners());
    }
}
