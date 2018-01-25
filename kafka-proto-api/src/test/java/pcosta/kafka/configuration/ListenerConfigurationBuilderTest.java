package pcosta.kafka.configuration;

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import org.junit.Test;
import pcosta.kafka.api.MessageFilter;
import pcosta.kafka.api.MessageListener;
import pcosta.kafka.api.MessageListenerConfiguration;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the {@link ListenerConfigurationBuilder}.
 *
 * @author Pedro Costa
 */
public class ListenerConfigurationBuilderTest {

    @Test(expected = NullPointerException.class)
    @SuppressWarnings("unchecked")
    public void test_invalidHandler() throws Exception {
        new ListenerConfigurationBuilder<>(mock(ReceiverConfigurationBuilder.class)).addHandler((MessageListener) null);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test_addHandler_success() throws Exception {
        new ListenerConfigurationBuilder<>(mock(ReceiverConfigurationBuilder.class))
                .addHandler(mock(MessageListener.class))
                .addHandler(mock(MessageListener.class));
    }

    @Test(expected = NullPointerException.class)
    public void test_invalidHandlers() throws Exception {
        new ListenerConfigurationBuilder<>(mock(ReceiverConfigurationBuilder.class)).addHandlers(null);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test_addHandlers_success() {
        final Collection<MessageListener> listeners = Collections.singletonList(mock(MessageListener.class));
        final Collection<MessageListener> listeners2 = Arrays.asList(mock(MessageListener.class), mock(MessageListener.class));
        new ListenerConfigurationBuilder<>(mock(ReceiverConfigurationBuilder.class)).addHandlers(listeners).addHandlers(listeners2);
    }

    @Test(expected = NullPointerException.class)
    @SuppressWarnings("unchecked")
    public void test_addHandlers_fail() throws Exception {
        final Collection<MessageListener> listeners = Collections.singletonList(mock(MessageListener.class));
        final Collection<MessageListener> listeners2 = Arrays.asList(mock(MessageListener.class), null);
        new ListenerConfigurationBuilder<>(mock(ReceiverConfigurationBuilder.class)).addHandlers(listeners).addHandlers(listeners2);
    }

    @Test(expected = NullPointerException.class)
    @SuppressWarnings("unchecked")
    public void test_invalidHandlerBuild() throws Exception {
        final ReceiverConfigurationBuilder receiverConfigurationBuilder = mock(ReceiverConfigurationBuilder.class);
        new ListenerConfigurationBuilder<>(receiverConfigurationBuilder)
                .withMessageType(Message.class)
                .buildListener();
    }

    @Test(expected = NullPointerException.class)
    @SuppressWarnings("unchecked")
    public void test_invalidFilter() throws Exception {
        new ListenerConfigurationBuilder<>(mock(ReceiverConfigurationBuilder.class)).addFilter((MessageFilter) null);
    }

    @Test
    public void test_addFilter_success() {
        new ListenerConfigurationBuilder<>(mock(ReceiverConfigurationBuilder.class))
                .addFilter(mock(MessageFilter.class))
                .addInitialOffset(-2L)
                .addTopicPartition(0)
                .addFilter(mock(MessageFilter.class));
    }

    @Test(expected = NullPointerException.class)
    public void test_invalidFilters() throws Exception {
        new ListenerConfigurationBuilder<>(mock(ReceiverConfigurationBuilder.class)).addFilters(null);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test_addFilters_success() {
        final Collection<MessageFilter> filters = Collections.singletonList(mock(MessageFilter.class));
        final Collection<MessageFilter> filters2 = Arrays.asList(mock(MessageFilter.class), mock(MessageFilter.class));
        new ListenerConfigurationBuilder<>(mock(ReceiverConfigurationBuilder.class))
                .addFilters(filters)
                .addInitialOffset(-2L)
                .addTopicPartition(0)
                .addFilters(filters2);
    }

    @Test(expected = NullPointerException.class)
    @SuppressWarnings("unchecked")
    public void test_addFilters_fail() throws Exception {
        final Collection<MessageFilter> filters = Collections.singletonList(mock(MessageFilter.class));
        final Collection<MessageFilter> filters2 = Arrays.asList(mock(MessageFilter.class), null);
        new ListenerConfigurationBuilder<>(mock(ReceiverConfigurationBuilder.class))
                .addFilters(filters)
                .addFilters(filters2);
    }

    @Test
    public void test_addTopics() {
        new ListenerConfigurationBuilder<>(mock(ReceiverConfigurationBuilder.class));
        new ListenerConfigurationBuilder<>(mock(ReceiverConfigurationBuilder.class))
                .addTopics("GoodTopic")
                .addTopics("GoodTopic", "GoodTopic2");
    }

    @Test(expected = NullPointerException.class)
    public void test_invalidTopic() throws Exception {
        new ListenerConfigurationBuilder<>(mock(ReceiverConfigurationBuilder.class)).addTopics((String[]) null);
    }

    @Test(expected = NullPointerException.class)
    @SuppressWarnings("unchecked")
    public void test_addTopics_null() throws Exception {
        new ListenerConfigurationBuilder<>(mock(ReceiverConfigurationBuilder.class))
                .addTopics("GoodTopic")
                .addTopics("GoodTopic", null);
    }

    @Test(expected = IllegalArgumentException.class)
    @SuppressWarnings("unchecked")
    public void test_addDuplicatedTopics_fail() throws Exception {
        new ListenerConfigurationBuilder<>(mock(ReceiverConfigurationBuilder.class))
                .addTopics("GoodTopic")
                .addTopics("GoodTopic", "GoodTopic");
    }

    @Test(expected = NullPointerException.class)
    @SuppressWarnings("unchecked")
    public void test_invalidMetadataBuild() throws Exception {
        final ReceiverConfigurationBuilder receiverConfigurationBuilder = mock(ReceiverConfigurationBuilder.class);
        final MessageListener listener = mock(MessageListener.class);
        new ListenerConfigurationBuilder<>(receiverConfigurationBuilder)
                .withMessageType(Message.class)
                .addHandler(listener)
                .buildListener();
    }

    @Test(expected = NullPointerException.class)
    public void test_invalidDeserializer() throws Exception {
        new ListenerConfigurationBuilder<>(mock(ReceiverConfigurationBuilder.class)).withMessageType(null);
    }

    @Test(expected = IllegalStateException.class)
    @SuppressWarnings("unchecked")
    public void test_defineDeserializerTwice() throws Exception {
        new ListenerConfigurationBuilder(mock(ReceiverConfigurationBuilder.class))
                .withMessageType(Message.class)
                .withMessageType(SomeMessage.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test_defineDeserializerTwice_same() {
        new ListenerConfigurationBuilder<>(mock(ReceiverConfigurationBuilder.class))
                .withMessageType(Message.class)
                .withMessageType(Message.class);
    }

    @Test(expected = NullPointerException.class)
    @SuppressWarnings("unchecked")
    public void test_invalidDeserializerBuild() throws Exception {
        final ReceiverConfigurationBuilder receiverConfigurationBuilder = mock(ReceiverConfigurationBuilder.class);
        final MessageListener listener = mock(MessageListener.class);

        new ListenerConfigurationBuilder<>(receiverConfigurationBuilder)
                .addTopics(new String[]{"GoodTopic"})
                .addHandler(listener)
                .buildListener();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void test_success_build() {
        final ReceiverConfigurationBuilder receiverConfigurationBuilder = mock(ReceiverConfigurationBuilder.class);
        final MessageListener listener = mock(MessageListener.class);

        new ListenerConfigurationBuilder<>(receiverConfigurationBuilder)
                .withMessageType(Message.class)
                .addTopics(new String[]{"GoodTopic"})
                .addHandler(listener)
                .buildListener();

        verify(receiverConfigurationBuilder, times(1)).addListenerConfiguration(any(MessageListenerConfiguration.class));
    }

    // test messages
    @SuppressWarnings("unchecked")
    private static abstract class SomeMessage extends GeneratedMessageV3 {
        @SuppressWarnings("unused")
        public static Parser<SomeMessage> PARSER = mock(Parser.class);
    }

}
