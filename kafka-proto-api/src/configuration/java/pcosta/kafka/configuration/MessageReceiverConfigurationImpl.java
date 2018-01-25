package pcosta.kafka.configuration;

import pcosta.kafka.api.MessageListenerConfiguration;
import pcosta.kafka.api.MessageReceiverConfiguration;
import pcosta.kafka.api.PlatformErrorListener;

import java.util.Collection;

/**
 * A simple implementation of the {@link MessageReceiverConfiguration}, which receives it's configuration
 * upon construction time.
 *
 * @author Pedro Costa
 */
class MessageReceiverConfigurationImpl implements MessageReceiverConfiguration {

    // the listeners configuration
    private final Collection<MessageListenerConfiguration<?>> listenerConfigurations;

    // the platform error listener
    private final PlatformErrorListener errorListener;

    /**
     * Default configuration constructor
     *
     * @param errorListener          the platform error listener
     * @param listenerConfigurations the receiver listeners configuration
     */
    MessageReceiverConfigurationImpl(final PlatformErrorListener errorListener, final Collection<MessageListenerConfiguration<?>> listenerConfigurations) {
        this.listenerConfigurations = listenerConfigurations;
        this.errorListener = errorListener;
    }

    @Override
    public PlatformErrorListener getErrorListener() {
        return errorListener;
    }

    @Override
    public Collection<MessageListenerConfiguration<?>> getListeners() {
        return listenerConfigurations;
    }
}
